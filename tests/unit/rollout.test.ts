import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';
import { StateDB } from '../../control-plane/src/db';
import { Scheduler } from '../../control-plane/src/scheduler';
import { Reconciler } from '../../control-plane/src/reconciler';
import { Replica } from '../../shared/src/types';

describe('Rolling Update Controller (maxSurge & maxUnavailable)', () => {
  let db: StateDB;
  let scheduler: Scheduler;
  let reconciler: Reconciler;
  const testDbPath = path.join(__dirname, 'test-rollout.db');

  beforeEach(() => {
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
    db = new StateDB(testDbPath);
    scheduler = new Scheduler(db);
    reconciler = new Reconciler(db, scheduler);

    db.upsertNode({
      name: 'node-1',
      cpuCapacity: 4000,
      memCapacity: 4096,
      cpuAllocated: 0,
      memAllocated: 0,
      status: 'Ready',
      lastHeartbeatAt: Date.now(),
    });

    // Deployment with 4 replicas, maxSurge: 1, maxUnavailable: 1
    db.upsertDeployment({
      name: 'web',
      currentRevision: 2, // updating to rev 2
      desiredReplicas: 4,
      selector: { app: 'web' },
      strategy: { maxSurge: 1, maxUnavailable: 1 },
    });

    db.insertRevision({
      deploymentName: 'web',
      revision: 1,
      specJson: JSON.stringify({
        metadata: { name: 'web' },
        spec: {
          replicas: 4,
          template: { spec: { containers: [{ name: 'web', image: 'nginx:1.20' }] } }
        }
      }),
      createdAt: Date.now() - 10000,
    });

    db.insertRevision({
      deploymentName: 'web',
      revision: 2,
      specJson: JSON.stringify({
        metadata: { name: 'web' },
        spec: {
          replicas: 4,
          template: { spec: { containers: [{ name: 'web', image: 'nginx:1.21' }] } }
        }
      }),
      createdAt: Date.now(),
    });

    // Initialize with 4 ready revision 1 replicas
    for (let i = 1; i <= 4; i++) {
      db.insertReplica({
        id: `web-rev1-${i}`,
        deploymentName: 'web',
        revision: 1,
        node: 'node-1',
        state: 'Ready',
        restartCount: 0,
        ready: true,
        containerPort: 80,
        hostPort: 30000 + i,
        lastStateChange: Date.now(),
        image: 'nginx:1.20',
        cpuRequest: 100,
        memRequest: 128,
      });
    }
  });

  afterEach(() => {
    reconciler.stop();
    db.close();
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
  });

  it('respects maxSurge and maxUnavailable invariants across rollout steps', () => {
    const desired = 4;
    const maxSurge = 1;
    const maxUnavailable = 1;

    // Step 1: Reconcile tick
    (reconciler as any).tick();

    let allReplicas = db.getReplicasForDeployment('web')
      .filter(r => r.state !== 'Terminated');

    // Total replicas must never exceed desired + maxSurge = 5
    expect(allReplicas.length).toBeLessThanOrEqual(desired + maxSurge);

    // Active ready replicas must never drop below desired - maxUnavailable = 3
    let readyCount = allReplicas.filter(r => r.ready).length;
    expect(readyCount).toBeGreaterThanOrEqual(desired - maxUnavailable);

    // One new replica of revision 2 should have been created (surge)
    const rev2Replicas = allReplicas.filter(r => r.revision === 2);
    expect(rev2Replicas.length).toBe(1);

    // Mark the new rev-2 replica as Ready (passing health checks)
    const newRep = rev2Replicas[0];
    newRep.ready = true;
    newRep.state = 'Ready';
    db.updateReplica(newRep);

    // Step 2: Next reconcile tick
    (reconciler as any).tick();

    allReplicas = db.getReplicasForDeployment('web')
      .filter(r => r.state !== 'Terminated');

    // Invariant check again
    expect(allReplicas.length).toBeLessThanOrEqual(desired + maxSurge);
    readyCount = allReplicas.filter(r => r.ready).length;
    expect(readyCount).toBeGreaterThanOrEqual(desired - maxUnavailable);
  });
});
