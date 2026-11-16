import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import type { AddressInfo } from 'net';
import type { Server } from 'http';
import { StateDB } from '../../control-plane/src/db';
import { Scheduler } from '../../control-plane/src/scheduler';
import { Reconciler, TERMINATION_DRAIN_MS } from '../../control-plane/src/reconciler';
import { GrpcServer } from '../../control-plane/src/grpc-server';
import { createRestApi } from '../../control-plane/src/api';
import { parseSpecs, SpecValidationError } from '../../shared/src/parser';
import type { Replica } from '../../shared/src/types';

// Regression tests for bugs found while reviewing Portside. Each test fails on
// the original code and passes after the fix.

const specJson = (text: string, replicas = 2) => JSON.stringify({
  metadata: { name: 'web' },
  spec: {
    replicas,
    selector: { matchLabels: { app: 'web' } },
    template: { spec: { containers: [{ name: 'w', image: 'echo', args: [`-text=${text}`], ports: [{ containerPort: 80 }] }] } },
  },
});

const deploymentYaml = (text: string, replicas = 2, strategy = 'maxSurge: 1\n      maxUnavailable: 1') => `
apiVersion: portside/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: ${replicas}
  selector:
    matchLabels:
      app: web
  strategy:
    type: RollingUpdate
    rollingUpdate:
      ${strategy}
  template:
    spec:
      containers:
        - name: w
          image: echo
          args: ["-text=${text}"]
          ports:
            - containerPort: 80
`;

function replica(over: Partial<Replica>): Replica {
  return {
    id: 'r', deploymentName: 'web', revision: 1, node: 'node-1', state: 'Ready',
    restartCount: 0, ready: true, containerPort: 80, hostPort: 30000,
    lastStateChange: Date.now(), generation: 1, image: 'echo', cpuRequest: 100, memRequest: 128,
    ...over,
  };
}

describe('Regression tests', () => {
  let db: StateDB;
  let scheduler: Scheduler;
  let reconciler: Reconciler;
  const tick = () => (reconciler as any).tick();
  const heartbeatAll = () => {
    for (const n of db.getAllNodes()) db.updateNodeHeartbeat(n.name, Date.now());
  };
  const active = (dep = 'web') => db.getReplicasForDeployment(dep)
    .filter(r => r.state !== 'Terminated' && r.state !== 'Terminating');

  beforeEach(() => {
    db = new StateDB(':memory:');
    scheduler = new Scheduler(db);
    reconciler = new Reconciler(db, scheduler);
    for (const name of ['node-1', 'node-2']) {
      db.upsertNode({
        name, cpuCapacity: 2000, memCapacity: 2048, cpuAllocated: 0, memAllocated: 0,
        status: 'Ready', lastHeartbeatAt: Date.now(),
      });
    }
  });

  afterEach(() => {
    vi.useRealTimers();
    reconciler.stop();
    db.close();
  });

  function deployment(currentRevision: number, desired: number, strategy = { maxSurge: 1, maxUnavailable: 1 }) {
    db.upsertDeployment({ name: 'web', currentRevision, desiredReplicas: desired, selector: { app: 'web' }, strategy });
  }

  it('bug 1: drain survives heartbeats and evicts replicas to other nodes', () => {
    deployment(1, 2);
    db.insertRevision({ deploymentName: 'web', revision: 1, specJson: specJson('v1'), createdAt: 1 });
    db.insertReplica(replica({ id: 'a', hostPort: 30000 }));
    db.insertReplica(replica({ id: 'b', hostPort: 30001 }));
    scheduler.recalculateAllocations();

    db.updateNodeStatus('node-1', 'Draining');
    reconciler.processHeartbeat('node-1', [], { cpuUsed: 0, memUsed: 0 });
    expect(db.getNode('node-1')!.status).toBe('Draining');

    vi.useFakeTimers({ toFake: ['Date'] });
    tick();
    expect(db.getReplica('a')!.state).toBe('Terminating');
    expect(db.getReplica('b')!.state).toBe('Terminating');
    // Replacements respect maxSurge = 1 while the evicted containers drain.
    expect(active()).toHaveLength(1);

    vi.setSystemTime(Date.now() + TERMINATION_DRAIN_MS + 100);
    heartbeatAll();
    tick();
    const replacements = active();
    expect(replacements).toHaveLength(2);
    expect(replacements.every(r => r.node === 'node-2')).toBe(true);
    expect(db.getNode('node-1')!.status).toBe('Draining');
  });

  it('bug 3: one crash counts as one restart, and the restart uses a new generation', () => {
    deployment(1, 1);
    db.insertRevision({ deploymentName: 'web', revision: 1, specJson: specJson('v1', 1), createdAt: 1 });
    db.insertReplica(replica({ id: 'x', generation: 1 }));
    const exited = [{ replicaId: 'x', state: 'exited', exitCode: 1, healthCheck: false, restartCount: 0, generation: 1 }];

    for (let i = 0; i < 5; i++) reconciler.processHeartbeat('node-1', exited, { cpuUsed: 0, memUsed: 0 });
    let x = db.getReplica('x')!;
    expect(x.restartCount).toBe(1);
    expect(x.state).toBe('CrashLoopBackOff');

    // Backoff expired: the tick (not a setTimeout) issues the restart.
    x.nextRestartAt = Date.now() - 1;
    db.updateReplica(x);
    reconciler.drainCommands('node-1');
    tick();
    x = db.getReplica('x')!;
    expect(x.state).toBe('Scheduling');
    expect(x.generation).toBe(2);
    const cmds = reconciler.drainCommands('node-1');
    expect(cmds.some(c => c.kind === 'start' && c.start.generation === 2)).toBe(true);

    // The old exited container (generation 1) is still reported until replaced: ignored.
    reconciler.processHeartbeat('node-1', exited, { cpuUsed: 0, memUsed: 0 });
    expect(db.getReplica('x')!.restartCount).toBe(1);
    expect(db.getReplica('x')!.state).toBe('Scheduling');
  });

  it('bug 4: RolloutCompleted is emitted exactly once per revision', () => {
    deployment(2, 2);
    db.insertRevision({ deploymentName: 'web', revision: 1, specJson: specJson('v1'), createdAt: 1 });
    db.insertRevision({ deploymentName: 'web', revision: 2, specJson: specJson('v2'), createdAt: 2 });
    db.insertReplica(replica({ id: 'o1', hostPort: 30000 }));
    db.insertReplica(replica({ id: 'o2', hostPort: 30001 }));

    vi.useFakeTimers({ toFake: ['Date'] });
    for (let t = 0; t < 20; t++) {
      vi.setSystemTime(Date.now() + 1000);
      heartbeatAll();
      tick();
      for (const r of db.getReplicasForDeployment('web')) {
        if (r.revision === 2 && (r.state === 'Scheduling' || r.state === 'Starting')) {
          r.state = 'Ready'; r.ready = true; db.updateReplica(r);
        }
      }
    }
    expect(active().every(r => r.revision === 2 && r.ready)).toBe(true);
    expect(db.getEvents().filter(e => e.kind === 'RolloutCompleted')).toHaveLength(1);
  });

  it('bug 5: a stale copy on the previous node is stopped and cannot change the replica', () => {
    deployment(1, 1);
    db.insertReplica(replica({ id: 'y', node: 'node-2', state: 'Scheduling', ready: false }));
    reconciler.processHeartbeat('node-1', [
      { replicaId: 'y', state: 'running', exitCode: 0, healthCheck: true, restartCount: 0, generation: 1 },
    ], { cpuUsed: 0, memUsed: 0 });

    expect(db.getReplica('y')!.state).toBe('Scheduling');
    const cmds = reconciler.drainCommands('node-1');
    expect(cmds).toEqual([{ kind: 'stop', stop: { replicaId: 'y', graceSeconds: 10 } }]);

    // Throttled: the next heartbeat does not queue another stop immediately.
    reconciler.processHeartbeat('node-1', [
      { replicaId: 'y', state: 'running', exitCode: 0, healthCheck: true, restartCount: 0, generation: 1 },
    ], { cpuUsed: 0, memUsed: 0 });
    expect(reconciler.drainCommands('node-1')).toHaveLength(0);
  });

  it('bug 6: maxSurge 0 with maxUnavailable 0 is rejected', () => {
    expect(() => parseSpecs(deploymentYaml('v1', 2, 'maxSurge: 0\n      maxUnavailable: 0')))
      .toThrow(SpecValidationError);
    expect(() => parseSpecs(deploymentYaml('v1', 2, 'maxSurge: 0\n      maxUnavailable: 1'))).not.toThrow();
  });

  it('lost start commands are re-sent for replicas stuck in Scheduling', () => {
    deployment(1, 1);
    db.insertRevision({ deploymentName: 'web', revision: 1, specJson: specJson('v1', 1), createdAt: 1 });
    db.insertReplica(replica({ id: 's', state: 'Scheduling', ready: false, lastStateChange: Date.now() - 11000 }));
    tick();
    const cmds = reconciler.drainCommands('node-1');
    expect(cmds.some(c => c.kind === 'start' && c.start.replicaId === 's' && c.start.generation === 1)).toBe(true);
  });

  it('control plane startup grace: nodes are not declared dead before agents can reconnect', () => {
    reconciler = new Reconciler(db, scheduler, { startupGraceMs: 5000 });
    db.upsertNode({
      name: 'node-1', cpuCapacity: 2000, memCapacity: 2048, cpuAllocated: 0, memAllocated: 0,
      status: 'Ready', lastHeartbeatAt: Date.now() - 60000,
    });
    tick();
    expect(db.getNode('node-1')!.status).toBe('Ready');
  });

  it('re-registering a node keeps its allocation instead of resetting it to zero', () => {
    deployment(1, 1);
    db.insertReplica(replica({ id: 'k', cpuRequest: 700, memRequest: 512 }));
    const grpcServer = new GrpcServer(db, reconciler, 0);
    (grpcServer as any).handleRegister(
      { request: { nodeName: 'node-1', cpuCapacity: 2000, memCapacity: 2048 } },
      () => {}
    );
    const node = db.getNode('node-1')!;
    expect(node.cpuAllocated).toBe(700);
    expect(node.memAllocated).toBe(512);
  });

  it('rolling update never exceeds surge or drops below availability, counting terminating containers', () => {
    const desired = 6;
    deployment(2, desired);
    db.insertRevision({ deploymentName: 'web', revision: 1, specJson: specJson('v1', desired), createdAt: 1 });
    db.insertRevision({ deploymentName: 'web', revision: 2, specJson: specJson('v2', desired), createdAt: 2 });
    for (let i = 0; i < desired; i++) {
      db.insertReplica(replica({ id: `old-${i}`, node: i % 2 ? 'node-1' : 'node-2', hostPort: 30000 + i }));
    }
    scheduler.recalculateAllocations();

    vi.useFakeTimers({ toFake: ['Date'] });
    let ticks = 0;
    while (ticks < 60) {
      vi.setSystemTime(Date.now() + 500);
      heartbeatAll();
      tick();
      ticks++;
      const rows = db.getReplicasForDeployment('web').filter(r => r.state !== 'Terminated');
      expect(rows.length).toBeLessThanOrEqual(desired + 1);                       // maxSurge
      expect(rows.filter(r => r.ready).length).toBeGreaterThanOrEqual(desired - 1); // maxUnavailable
      for (const r of rows) {
        if (r.revision === 2 && (r.state === 'Scheduling' || r.state === 'Starting')) {
          r.state = 'Ready'; r.ready = true; db.updateReplica(r);
        }
      }
      const done = active();
      if (done.length === desired && done.every(r => r.revision === 2 && r.ready)) break;
    }
    expect(active().every(r => r.revision === 2)).toBe(true);
    expect(TERMINATION_DRAIN_MS).toBeGreaterThan(1000); // longer than the proxy refresh
  });

  describe('REST API', () => {
    let server: Server;
    let base: string;

    beforeEach(async () => {
      const app = createRestApi(db, reconciler, scheduler);
      server = await new Promise<Server>(resolve => {
        const s = app.listen(0, () => resolve(s));
      });
      base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    });
    afterEach(() => new Promise<void>(resolve => server.close(() => resolve())));

    const apply = (yaml: string) => fetch(`${base}/v1/apply`, {
      method: 'POST', headers: { 'Content-Type': 'application/x-yaml' }, body: yaml,
    });

    it('bug 2: apply after a rollback succeeds, and rollback creates a new revision', async () => {
      expect((await apply(deploymentYaml('v1'))).status).toBe(200);
      expect((await apply(deploymentYaml('v2'))).status).toBe(200);

      const rb = await fetch(`${base}/v1/deployments/web/rollback`, { method: 'POST' });
      expect(rb.status).toBe(200);
      expect((await rb.json() as any).revision).toBe(3);
      expect(JSON.parse(db.getRevision('web', 3)!.specJson).spec.template)
        .toEqual(JSON.parse(db.getRevision('web', 1)!.specJson).spec.template);

      const res = await apply(deploymentYaml('v3'));
      expect(res.status).toBe(200);
      expect(db.getDeployment('web')!.currentRevision).toBe(4);
    });

    it('re-applying an unchanged template does not start a rollout', async () => {
      await apply(deploymentYaml('v1', 2));
      const res = await apply(deploymentYaml('v1', 5));
      expect((await res.json() as any).applied[0].action).toBe('configured');
      expect(db.getDeployment('web')!.currentRevision).toBe(1);
      expect(db.getDeployment('web')!.desiredReplicas).toBe(5);
    });

    it('bug 6: the API rejects maxSurge 0 with maxUnavailable 0', async () => {
      const res = await apply(deploymentYaml('v1', 2, 'maxSurge: 0\n      maxUnavailable: 0'));
      expect(res.status).toBe(400);
    });

    it('uncordon makes a drained node schedulable again', async () => {
      await fetch(`${base}/v1/nodes/node-1/drain`, { method: 'POST' });
      expect(db.getNode('node-1')!.status).toBe('Draining');
      const res = await fetch(`${base}/v1/nodes/node-1/uncordon`, { method: 'POST' });
      expect(res.status).toBe(200);
      expect(db.getNode('node-1')!.status).toBe('Ready');
    });
  });
});
