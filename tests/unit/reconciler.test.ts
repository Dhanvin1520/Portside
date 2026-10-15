import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fs from 'fs';
import path from 'path';
import { StateDB } from '../../control-plane/src/db';
import { Scheduler } from '../../control-plane/src/scheduler';
import { Reconciler } from '../../control-plane/src/reconciler';

describe('Level-Triggered Reconcile Loop', () => {
  let db: StateDB;
  let scheduler: Scheduler;
  let reconciler: Reconciler;
  const testDbPath = path.join(__dirname, 'test-reconciler.db');

  beforeEach(() => {
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
    db = new StateDB(testDbPath);
    scheduler = new Scheduler(db);
    reconciler = new Reconciler(db, scheduler);

    // Register 2 nodes
    db.upsertNode({
      name: 'node-1',
      cpuCapacity: 2000,
      memCapacity: 2048,
      cpuAllocated: 0,
      memAllocated: 0,
      status: 'Ready',
      lastHeartbeatAt: Date.now(),
    });
    db.upsertNode({
      name: 'node-2',
      cpuCapacity: 2000,
      memCapacity: 2048,
      cpuAllocated: 0,
      memAllocated: 0,
      status: 'Ready',
      lastHeartbeatAt: Date.now(),
    });
  });

  afterEach(() => {
    reconciler.stop();
    db.close();
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
  });

  it('reconciles under-provisioned deployment by creating and scheduling replicas', () => {
    // Insert deployment desiring 2 replicas
    db.upsertDeployment({
      name: 'web',
      currentRevision: 1,
      desiredReplicas: 2,
      selector: { app: 'web' },
      strategy: { maxSurge: 1, maxUnavailable: 1 },
    });

    db.insertRevision({
      deploymentName: 'web',
      revision: 1,
      specJson: JSON.stringify({
        metadata: { name: 'web' },
        spec: {
          replicas: 2,
          template: {
            spec: {
              containers: [{ name: 'web', image: 'nginx', ports: [{ containerPort: 80 }] }]
            }
          }
        }
      }),
      createdAt: Date.now(),
    });

    // Execute one reconcile tick manually via internal method or starting reconciler
    // We can call private tick or trigger loop
    (reconciler as any).tick();

    const replicas = db.getReplicasForDeployment('web');
    expect(replicas.length).toBe(2);

    // Both should now be scheduled on nodes
    expect(replicas.every(r => r.node !== null)).toBe(true);

    // Verify commands were generated for nodes
    const node1Cmds = reconciler.drainCommands('node-1');
    const node2Cmds = reconciler.drainCommands('node-2');
    expect(node1Cmds.length + node2Cmds.length).toBe(2);
    expect(node1Cmds[0]?.kind || node2Cmds[0]?.kind).toBe('start');
  });

  it('processes heartbeats and detects crashed containers with backoff', () => {
    db.upsertDeployment({
      name: 'worker',
      currentRevision: 1,
      desiredReplicas: 1,
      selector: { app: 'worker' },
      strategy: { maxSurge: 1, maxUnavailable: 1 },
    });

    // Setup 1 scheduled replica
    db.insertReplica({
      id: 'rep-crasher',
      deploymentName: 'worker',
      revision: 1,
      node: 'node-1',
      state: 'Running',
      restartCount: 0,
      ready: true,
      containerPort: 8080,
      hostPort: 30001,
      lastStateChange: Date.now(),
      image: 'faulty-app',
      cpuRequest: 100,
      memRequest: 128,
    });

    // Send heartbeat indicating container exited with error code 1
    reconciler.processHeartbeat('node-1', [
      {
        replicaId: 'rep-crasher',
        state: 'exited',
        exitCode: 1,
        healthCheck: false,
        restartCount: 0,
      }
    ], { cpuUsed: 0, memUsed: 0 });

    const updated = db.getReplica('rep-crasher')!;
    expect(updated.state).toBe('CrashLoopBackOff');
    expect(updated.restartCount).toBe(1);
    expect(updated.nextRestartAt).toBeDefined();
  });

  it('detects node heartbeat timeout and reschedules workloads off failed node', () => {
    db.upsertDeployment({
      name: 'critical-api',
      currentRevision: 1,
      desiredReplicas: 1,
      selector: { app: 'critical-api' },
      strategy: { maxSurge: 1, maxUnavailable: 1 },
    });

    // Mark node-1 heartbeat as 10 seconds in the past (> 3s threshold)
    const staleTime = Date.now() - 10000;
    db.upsertNode({
      name: 'node-1',
      cpuCapacity: 2000,
      memCapacity: 2048,
      cpuAllocated: 500,
      memAllocated: 512,
      status: 'Ready',
      lastHeartbeatAt: staleTime,
    });

    db.insertReplica({
      id: 'rep-on-failing-node',
      deploymentName: 'critical-api',
      revision: 1,
      node: 'node-1',
      state: 'Ready',
      restartCount: 0,
      ready: true,
      containerPort: 8080,
      hostPort: 30005,
      lastStateChange: Date.now(),
      image: 'api-service',
      cpuRequest: 500,
      memRequest: 512,
    });

    // Run tick
    (reconciler as any).tick();

    // Node-1 should be marked NotReady
    const node1 = db.getNode('node-1')!;
    expect(node1.status).toBe('NotReady');

    // Replica should be evicted from node-1 and moved to Pending/rescheduled onto node-2
    const replica = db.getReplica('rep-on-failing-node')!;
    expect(replica.node).not.toBe('node-1');
  });
});
