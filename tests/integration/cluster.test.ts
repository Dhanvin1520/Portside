import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';
import type { AddressInfo } from 'net';
import type { Server } from 'http';
import { StateDB } from '../../control-plane/src/db';
import { Scheduler } from '../../control-plane/src/scheduler';
import { Reconciler } from '../../control-plane/src/reconciler';
import { GrpcServer } from '../../control-plane/src/grpc-server';
import { createRestApi } from '../../control-plane/src/api';
import { Agent, ContainerRuntime, StartParams } from '../../agent/src/agent';
import type { GrpcContainerStatus } from '../../shared/src/types';

// ─────────────────────────────────────────────────────────────────────────────
// Simulated cluster test
//
// Runs the REAL control plane (SQLite, scheduler, reconcile loop, gRPC server,
// REST API) and three REAL agents (registration, heartbeat stream, reconnect
// loop, command handling) in one process. Only Docker is replaced by an
// in-memory FakeRuntime, so this runs anywhere, including CI without Docker.
//
// Timings printed at the end measure the orchestration logic with instant
// containers. They are NOT a substitute for the Docker-based chaos harness.
// ─────────────────────────────────────────────────────────────────────────────

interface FakeContainer {
  generation: number;
  state: 'running' | 'exited';
  exitCode: number;
  healthy: boolean;
  text: string;
}

class FakeRuntime implements ContainerRuntime {
  containers = new Map<string, FakeContainer>();
  startCalls = 0;

  constructor(public readonly node: string, private readyDelayMs = 200) {}

  async startContainer(p: StartParams): Promise<void> {
    const existing = this.containers.get(p.replicaId);
    if (existing && existing.generation === p.generation && existing.state === 'running') return;
    this.startCalls++;
    const c: FakeContainer = {
      generation: p.generation, state: 'running', exitCode: 0, healthy: false,
      text: (p.args.find(a => a.startsWith('-text=')) || '').slice(6),
    };
    this.containers.set(p.replicaId, c);
    setTimeout(() => { if (this.containers.get(p.replicaId) === c) c.healthy = true; }, this.readyDelayMs);
  }

  async stopContainer(replicaId: string): Promise<void> {
    this.containers.delete(replicaId);
  }

  async getContainerStatuses(): Promise<GrpcContainerStatus[]> {
    return [...this.containers.entries()].map(([replicaId, c]) => ({
      replicaId, state: c.state, exitCode: c.exitCode,
      healthCheck: c.state === 'running' && c.healthy, restartCount: 0, generation: c.generation,
    }));
  }

  getResourceUsage() {
    return { cpuUsed: this.containers.size * 100, memUsed: this.containers.size * 128 };
  }

  crash(replicaId: string): void {
    const c = this.containers.get(replicaId);
    if (c) { c.state = 'exited'; c.exitCode = 1; c.healthy = false; }
  }

  running(): string[] {
    return [...this.containers.entries()].filter(([, c]) => c.state === 'running').map(([id]) => id);
  }
}

class ControlPlane {
  db!: StateDB;
  scheduler!: Scheduler;
  reconciler!: Reconciler;
  grpc!: GrpcServer;
  http!: Server;
  api = '';

  constructor(private dbPath: string, private grpcPort: number) {}

  async start(): Promise<void> {
    this.db = new StateDB(this.dbPath);
    this.scheduler = new Scheduler(this.db);
    this.reconciler = new Reconciler(this.db, this.scheduler, { startupGraceMs: 5000 });
    this.scheduler.recalculateAllocations();
    this.grpc = new GrpcServer(this.db, this.reconciler, this.grpcPort);
    await this.grpc.start();
    this.reconciler.start();
    const app = createRestApi(this.db, this.reconciler, this.scheduler);
    this.http = await new Promise<Server>(resolve => {
      const s = app.listen(0, () => resolve(s));
    });
    this.api = `http://127.0.0.1:${(this.http.address() as AddressInfo).port}`;
  }

  async stop(): Promise<void> {
    this.reconciler.stop();
    this.grpc.stop();
    await new Promise<void>(resolve => this.http.close(() => resolve()));
    this.db.close();
  }
}

const deploymentYaml = (text: string, replicas = 6) => `
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
      maxSurge: 1
      maxUnavailable: 1
  template:
    spec:
      containers:
        - name: web
          image: echo
          args: ["-text=${text}"]
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
          ports:
            - containerPort: 5678
          readinessProbe:
            httpGet:
              path: /
              port: 5678
            periodSeconds: 1
`;

async function waitFor(what: string, cond: () => boolean, timeoutMs = 20000): Promise<number> {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (cond()) return Date.now() - t0;
    await new Promise(r => setTimeout(r, 50));
  }
  throw new Error(`Timed out after ${timeoutMs} ms waiting for: ${what}`);
}

describe('Simulated 3-node cluster (real control plane + real agents, fake containers)', () => {
  const DESIRED = 6;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'portside-it-'));
  const dbPath = path.join(dir, 'portside.db');
  const grpcPort = 20000 + Math.floor(Math.random() * 20000);
  const cp = new ControlPlane(dbPath, grpcPort);
  const runtimes = new Map<string, FakeRuntime>();
  const agents = new Map<string, Agent>();
  const timings: Record<string, number> = {};

  const newAgent = (node: string) => {
    const agent = new Agent(
      { nodeName: node, controlPlane: `127.0.0.1:${grpcPort}`, cpuCapacity: 2000, memCapacity: 2048 },
      runtimes.get(node)!
    );
    agents.set(node, agent);
    return agent.start();
  };

  const db = () => cp.db;
  const activeReplicas = () => db().getReplicasForDeployment('web')
    .filter(r => r.state !== 'Terminated' && r.state !== 'Terminating');
  const readyCount = () => activeReplicas().filter(r => r.ready).length;
  const allReady = (rev?: number) => {
    const act = activeReplicas();
    return act.length === DESIRED && act.every(r => r.ready && (rev === undefined || r.revision === rev));
  };
  const runningContainers = () => [...runtimes.values()].reduce((n, rt) => n + rt.running().length, 0);
  const apply = (yaml: string) => fetch(`${cp.api}/v1/apply`, {
    method: 'POST', headers: { 'Content-Type': 'application/x-yaml' }, body: yaml,
  });
  /** Replica IDs that run on more than one node, plus containers on the wrong node. */
  const duplicatesAndStrays = () => {
    const owners = new Map(activeReplicas().map(r => [r.id, r.node]));
    let dup = 0;
    let stray = 0;
    const seen = new Map<string, number>();
    for (const rt of runtimes.values()) {
      for (const id of rt.running()) {
        seen.set(id, (seen.get(id) || 0) + 1);
        if (owners.get(id) !== rt.node) stray++;
      }
    }
    for (const n of seen.values()) if (n > 1) dup++;
    return { dup, stray };
  };

  beforeAll(async () => {
    await cp.start();
    for (const node of ['node-1', 'node-2', 'node-3']) {
      runtimes.set(node, new FakeRuntime(node));
      await newAgent(node);
    }
    await waitFor('3 Ready nodes', () =>
      db().getAllNodes().filter(n => n.status === 'Ready').length === 3);
  }, 30000);

  afterAll(async () => {
    for (const a of agents.values()) await a.stop();
    await cp.stop().catch(() => {});
    fs.rmSync(dir, { recursive: true, force: true });
    console.log('\nSimulated cluster timings (fake containers, real control plane):');
    console.table(Object.fromEntries(Object.entries(timings).map(([k, v]) => [k, `${v} ms`])));
  });

  it('deploys replicas and spreads them evenly across nodes', async () => {
    const res = await apply(deploymentYaml('v1'));
    expect(res.status).toBe(200);
    timings['deploy 6 replicas to all ready'] = await waitFor('all replicas ready', () => allReady());
    const perNode = ['node-1', 'node-2', 'node-3'].map(n => activeReplicas().filter(r => r.node === n).length);
    expect(perNode).toEqual([2, 2, 2]);
  }, 30000);

  it('restarts a crashed container once and it is ready again within 5 s', async () => {
    const victim = activeReplicas()[0];
    runtimes.get(victim.node!)!.crash(victim.id);
    timings['crashed container back to ready'] = await waitFor('crashed replica ready again', () => {
      const r = db().getReplica(victim.id)!;
      return r.restartCount >= 1 && r.ready;
    }, 10000);
    expect(timings['crashed container back to ready']).toBeLessThan(5000);
    await new Promise(r => setTimeout(r, 2500)); // more heartbeats must not count more crashes
    expect(db().getReplica(victim.id)!.restartCount).toBe(1);
  }, 20000);

  it('rolls out a new revision without breaking maxSurge / maxUnavailable', async () => {
    const violations: string[] = [];
    const sampler = setInterval(() => {
      const running = runningContainers();
      const ready = readyCount();
      if (running > DESIRED + 1) violations.push(`running containers ${running} > ${DESIRED + 1}`);
      if (ready < DESIRED - 1) violations.push(`ready replicas ${ready} < ${DESIRED - 1}`);
    }, 25);
    try {
      expect((await apply(deploymentYaml('v2'))).status).toBe(200);
      timings['rolling update 6 replicas'] = await waitFor('rollout to revision 2', () =>
        db().getEvents().some(e => e.kind === 'RolloutCompleted' && e.message.startsWith('Revision 2 ')), 60000);
    } finally {
      clearInterval(sampler);
    }
    expect(violations).toEqual([]);
    expect(allReady(2)).toBe(true);
    // The last stop command reaches the agent within one command flush (200 ms).
    await waitFor('only v2 containers left', () =>
      [...runtimes.values()].every(rt => [...rt.containers.values()].every(c => c.text === 'v2')), 3000);
  }, 70000);

  it('re-applying the same spec does not start a rollout', async () => {
    const res = await apply(deploymentYaml('v2'));
    expect((await res.json() as any).applied[0].action).toBe('configured');
    expect(db().getDeployment('web')!.currentRevision).toBe(2);
  });

  it('rollback creates a new revision, and applying afterwards still works', async () => {
    const rb = await fetch(`${cp.api}/v1/deployments/web/rollback`, { method: 'POST' });
    expect(rb.status).toBe(200);
    expect(db().getDeployment('web')!.currentRevision).toBe(3);
    await waitFor('rollback complete', () => allReady(3), 60000);

    const res = await apply(deploymentYaml('v4'));
    expect(res.status).toBe(200);
    expect(db().getDeployment('web')!.currentRevision).toBe(4);
    await waitFor('rollout to revision 4', () => allReady(4), 60000);
  }, 130000);

  it('drain moves replicas off the node, and stays drained despite heartbeats', async () => {
    await fetch(`${cp.api}/v1/nodes/node-1/drain`, { method: 'POST' });
    timings['drain node to all ready elsewhere'] = await waitFor('node-1 empty and all ready', () =>
      allReady() && activeReplicas().every(r => r.node !== 'node-1') &&
      runtimes.get('node-1')!.running().length === 0, 30000);
    expect(db().getNode('node-1')!.status).toBe('Draining');

    const res = await fetch(`${cp.api}/v1/nodes/node-1/uncordon`, { method: 'POST' });
    expect(res.status).toBe(200);
    expect(db().getNode('node-1')!.status).toBe('Ready');
  }, 40000);

  it('node failure: replicas are rescheduled, and stale copies are stopped when the node returns', async () => {
    const victimNode = 'node-2';
    const victims = activeReplicas().filter(r => r.node === victimNode).map(r => r.id);
    expect(victims.length).toBeGreaterThan(0);

    const t0 = Date.now();
    await agents.get(victimNode)!.stop(); // node goes silent, its containers keep "running"
    await waitFor('node NotReady', () => db().getNode(victimNode)!.status === 'NotReady', 10000);
    timings['node failure: detected NotReady'] = Date.now() - t0;
    await waitFor('replicas moved off', () =>
      victims.every(id => db().getReplica(id)!.node !== victimNode), 10000);
    timings['node failure: replicas rescheduled'] = Date.now() - t0;
    await waitFor('all ready on other nodes', () => allReady(), 15000);
    timings['node failure: all replicas ready again'] = Date.now() - t0;
    expect(timings['node failure: replicas rescheduled']).toBeLessThan(5000);

    // While it was away, the old node still runs copies: these are strays.
    expect(duplicatesAndStrays().stray).toBeGreaterThan(0);

    // The node comes back with its stale containers. Fencing must stop them.
    const t1 = Date.now();
    await newAgent(victimNode);
    await waitFor('node Ready again', () => db().getNode(victimNode)!.status === 'Ready', 10000);
    await waitFor('no duplicates or strays', () => {
      const { dup, stray } = duplicatesAndStrays();
      return dup === 0 && stray === 0;
    }, 15000);
    timings['rejoin: stale copies stopped'] = Date.now() - t1;
    expect(allReady()).toBe(true);
  }, 60000);

  it('control plane restart: agents reconnect and no container is restarted', async () => {
    const startsBefore = [...runtimes.values()].reduce((n, rt) => n + rt.startCalls, 0);
    const restartAt = Date.now();

    await cp.stop();
    await new Promise(r => setTimeout(r, 1500));
    await cp.start();

    timings['control plane restart: nodes reconnected'] = await waitFor('all nodes heartbeating', () =>
      db().getAllNodes().every(n => n.status === 'Ready' && n.lastHeartbeatAt > restartAt + 1500), 15000);
    await new Promise(r => setTimeout(r, 6000)); // past the startup grace period
    expect(allReady()).toBe(true);
    expect(db().getEvents().filter(e => e.kind === 'NodeNotReady' && e.time > restartAt)).toEqual([]);
    const startsAfter = [...runtimes.values()].reduce((n, rt) => n + rt.startCalls, 0);
    expect(startsAfter - startsBefore).toBe(0);
    expect(duplicatesAndStrays()).toEqual({ dup: 0, stray: 0 });
  }, 40000);

  it('rejects a strategy that can never make progress', async () => {
    const yaml = deploymentYaml('v5').replace('maxSurge: 1', 'maxSurge: 0').replace('maxUnavailable: 1', 'maxUnavailable: 0');
    expect((await apply(yaml)).status).toBe(400);
  });
});
