import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';
import { StateDB } from '../../control-plane/src/db';
import { Scheduler } from '../../control-plane/src/scheduler';
import { Replica } from '../../shared/src/types';

describe('Resource-Aware Scheduler', () => {
  let db: StateDB;
  let scheduler: Scheduler;
  const testDbPath = path.join(__dirname, 'test-scheduler.db');

  beforeEach(() => {
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
    db = new StateDB(testDbPath);
    scheduler = new Scheduler(db);

    // Register 3 nodes
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
    db.upsertNode({
      name: 'node-3',
      cpuCapacity: 2000,
      memCapacity: 2048,
      cpuAllocated: 0,
      memAllocated: 0,
      status: 'Ready',
      lastHeartbeatAt: Date.now(),
    });
  });

  afterEach(() => {
    db.close();
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
  });

  it('schedules replicas with spread across nodes', () => {
    const assignedNodes: string[] = [];

    for (let i = 0; i < 3; i++) {
      const replica: Replica = {
        id: `rep-${i}`,
        deploymentName: 'web',
        revision: 1,
        node: null,
        state: 'Pending',
        restartCount: 0,
        ready: false,
        containerPort: 8080,
        hostPort: 0,
        lastStateChange: Date.now(),
        image: 'nginx',
        cpuRequest: 500,
        memRequest: 512,
      };

      const res = scheduler.schedule(replica);
      expect(res.success).toBe(true);
      expect(res.nodeName).toBeDefined();
      assignedNodes.push(res.nodeName!);
    }

    // Since nodes start identical and anti-affinity/least-allocated scores penalize same node,
    // 3 replicas should be placed across the 3 unique nodes
    const unique = new Set(assignedNodes);
    expect(unique.size).toBe(3);
  });

  it('leaves replica pending when resources are insufficient with reason', () => {
    const hugeReplica: Replica = {
      id: 'huge-rep',
      deploymentName: 'heavy',
      revision: 1,
      node: null,
      state: 'Pending',
      restartCount: 0,
      ready: false,
      containerPort: 8080,
      hostPort: 0,
      lastStateChange: Date.now(),
      image: 'ml-model',
      cpuRequest: 8000, // exceeds any node's 2000m
      memRequest: 16384,
    };

    const res = scheduler.schedule(hugeReplica);
    expect(res.success).toBe(false);
    expect(res.reason).toMatch(/insufficient/);
  });

  it('never schedules onto NotReady or Draining nodes', () => {
    db.updateNodeStatus('node-1', 'NotReady');
    db.updateNodeStatus('node-2', 'Draining');

    const replica: Replica = {
      id: 'rep-solo',
      deploymentName: 'web',
      revision: 1,
      node: null,
      state: 'Pending',
      restartCount: 0,
      ready: false,
      containerPort: 8080,
      hostPort: 0,
      lastStateChange: Date.now(),
      image: 'nginx',
      cpuRequest: 500,
      memRequest: 512,
    };

    const res = scheduler.schedule(replica);
    expect(res.success).toBe(true);
    expect(res.nodeName).toBe('node-3'); // only Ready node
  });

  it('correctly releases resources upon replica deletion', () => {
    const replica: Replica = {
      id: 'rep-temp',
      deploymentName: 'web',
      revision: 1,
      node: null,
      state: 'Pending',
      restartCount: 0,
      ready: false,
      containerPort: 8080,
      hostPort: 0,
      lastStateChange: Date.now(),
      image: 'nginx',
      cpuRequest: 1000,
      memRequest: 1024,
    };

    const res = scheduler.schedule(replica);
    replica.node = res.nodeName!;

    const nodeBefore = db.getNode(res.nodeName!)!;
    expect(nodeBefore.cpuAllocated).toBe(1000);
    expect(nodeBefore.memAllocated).toBe(1024);

    scheduler.release(replica);

    const nodeAfter = db.getNode(res.nodeName!)!;
    expect(nodeAfter.cpuAllocated).toBe(0);
    expect(nodeAfter.memAllocated).toBe(0);
  });
});
