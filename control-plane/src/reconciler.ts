import { v4 as uuidv4 } from 'uuid';
import {
  Replica,
  Deployment,
  DeploymentSpec,
  GrpcCommand,
  GrpcContainerStatus,
  createLogger,
  parseCpuString,
  parseMemoryString,
} from '@portside/shared';
import { StateDB } from './db';
import { Scheduler } from './scheduler';

const log = createLogger('reconciler');

// ─────────────────────────────────────────────────────────────────────────────
// Reconcile Loop
//
// Level-triggered reconciliation: every tick, we compare desired state
// (what deployments say) with actual state (what replicas report), and
// issue commands to converge.
//
// Every decision below is a function of what is stored in the database plus
// the current time. Nothing important lives in a timer or depends on a single
// message arriving: if a command is lost or the control plane restarts, the
// next tick re-discovers the drift and fixes it.
//
// Each tick:
// 1. Mark nodes NotReady after missed heartbeats and reschedule their replicas
// 2. Evict replicas from Draining nodes
// 3. Finish terminations whose traffic-drain delay has passed (send stop)
// 4. Restart crashed replicas whose backoff has expired
// 5. Re-send start commands the agent never acted on
// 6. Per deployment: scale, drive rolling updates, schedule Pending replicas
// 7. Garbage-collect old Terminated rows
// ─────────────────────────────────────────────────────────────────────────────

/** Exponential backoff constants */
export const INITIAL_BACKOFF_MS = 1000;
export const MAX_BACKOFF_MS = 30000;
export const BACKOFF_MULTIPLIER = 2;

/** Heartbeat failure detection */
export const HEARTBEAT_INTERVAL_MS = 1000;
export const HEARTBEAT_MISS_THRESHOLD = 3;
export const NODE_TIMEOUT_MS = HEARTBEAT_INTERVAL_MS * HEARTBEAT_MISS_THRESHOLD;

/** Loop timing */
export const TICK_INTERVAL_MS = 1000;
/** Re-send a start if the agent has not reported the replica after this long. */
export const START_RESEND_MS = 10000;
/** Re-send a stop for a container that is still reported after this long. */
export const STOP_RESEND_MS = 10000;
/**
 * A replica that was serving traffic stays running this long after it is
 * marked not-ready, so load balancers (which refresh every 1 s) stop routing
 * to it before the container gets SIGTERM. Same idea as a Kubernetes preStop
 * sleep.
 */
export const TERMINATION_DRAIN_MS = 1500;
/** Terminated replica rows are deleted after this long. */
export const TERMINATED_GC_MS = 5 * 60 * 1000;

export interface ReconcilerOptions {
  /**
   * After the control plane starts, give agents this long to reconnect before
   * any node can be declared dead. Without it, a control plane that was down
   * for more than NODE_TIMEOUT_MS would mark every node NotReady on its first
   * tick and reschedule the whole cluster.
   */
  startupGraceMs: number;
  tickIntervalMs: number;
}

const DEFAULT_OPTIONS: ReconcilerOptions = {
  startupGraceMs: 0,
  tickIntervalMs: TICK_INTERVAL_MS,
};

export class Reconciler {
  private pendingCommands: Map<string, GrpcCommand[]> = new Map(); // nodeName -> commands
  private lastStopSent: Map<string, number> = new Map();           // "node/replica" -> ts
  private running = false;
  private tickInterval: ReturnType<typeof setInterval> | null = null;
  private startedAt = Date.now();
  private readonly options: ReconcilerOptions;

  constructor(
    private db: StateDB,
    private scheduler: Scheduler,
    options: Partial<ReconcilerOptions> = {}
  ) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.startedAt = Date.now();
    log.info('Reconcile loop started', { startupGraceMs: this.options.startupGraceMs });

    this.tickInterval = setInterval(() => {
      try {
        this.tick();
      } catch (err) {
        log.error('Reconcile tick error', { error: (err as Error).message });
      }
    }, this.options.tickIntervalMs);
  }

  stop(): void {
    this.running = false;
    if (this.tickInterval) {
      clearInterval(this.tickInterval);
      this.tickInterval = null;
    }
    log.info('Reconcile loop stopped');
  }

  /**
   * Get and clear pending commands for a node.
   * Called by the gRPC stream handler to send commands to agents.
   */
  drainCommands(nodeName: string): GrpcCommand[] {
    const cmds = this.pendingCommands.get(nodeName) || [];
    this.pendingCommands.delete(nodeName);
    return cmds;
  }

  // ── Heartbeats ───────────────────────────────────────────────────────────

  /**
   * Process a heartbeat from an agent: update the node and replica states.
   * A heartbeat is a full status report (not a delta), so it is safe to
   * process the same information many times.
   */
  processHeartbeat(
    nodeName: string,
    containers: GrpcContainerStatus[],
    _used: { cpuUsed: number; memUsed: number }
  ): void {
    const before = this.db.getNode(nodeName);
    this.db.updateNodeHeartbeat(nodeName, Date.now());
    if (before?.status === 'NotReady') {
      log.info('Node heartbeats resumed, marking Ready', { node: nodeName });
      this.db.addEvent('NodeReady', `node/${nodeName}`, 'Node heartbeats resumed');
    }

    for (const c of containers) {
      const replica = this.db.getReplica(c.replicaId);

      if (!replica) {
        this.stopOnNode(nodeName, c.replicaId, 10, 'orphaned container (no such replica)');
        continue;
      }

      // Fencing: only the node the replica is assigned to may run it. A node
      // that comes back after a crash or partition may still be running a
      // copy of a replica that has since been moved elsewhere.
      if (replica.node !== nodeName) {
        this.stopOnNode(nodeName, c.replicaId, 10,
          `stale copy (replica is assigned to ${replica.node ?? 'no node'})`);
        continue;
      }

      if (replica.state === 'Terminated') {
        if (c.state === 'running') {
          this.stopOnNode(nodeName, c.replicaId, 30, 'terminated replica still running');
        }
        continue;
      }
      if (replica.state === 'Terminating') continue; // stop is sent after the drain delay

      // Ignore reports about an older container instance of this replica
      // (for example the exited container that a restart is replacing).
      if ((c.generation ?? 0) < replica.generation) continue;

      this.applyContainerReport(replica, c, nodeName);
    }
  }

  private applyContainerReport(replica: Replica, c: GrpcContainerStatus, nodeName: string): void {
    const now = Date.now();

    if (c.state === 'running') {
      if (c.healthCheck) {
        if (replica.state !== 'Ready' || !replica.ready) {
          replica.state = 'Ready';
          replica.ready = true;
          replica.lastStateChange = now;
          this.db.updateReplica(replica);
          this.db.addEvent('Ready', `replica/${replica.id}`, `Replica is ready on node ${nodeName}`);
        }
      } else if (replica.state === 'Ready') {
        replica.state = 'Unhealthy';
        replica.ready = false;
        replica.lastStateChange = now;
        this.db.updateReplica(replica);
        this.db.addEvent('Unhealthy', `replica/${replica.id}`,
          `Replica failed health check on node ${nodeName}`);
      } else if (replica.state === 'Scheduling') {
        // Running, first health check not passed yet.
        replica.state = 'Starting';
        replica.lastStateChange = now;
        this.db.updateReplica(replica);
      }
    } else if (c.state === 'starting') {
      if (replica.state === 'Scheduling') {
        replica.state = 'Starting';
        replica.lastStateChange = now;
        this.db.updateReplica(replica);
      }
    } else if (c.state === 'exited') {
      // Level-triggered: the agent keeps reporting "exited" every second
      // until the container is replaced. Count the crash only once.
      if (replica.state !== 'CrashLoopBackOff') {
        this.handleCrashedReplica(replica, c.exitCode, nodeName);
      }
    }
  }

  // ── Main reconcile tick ──────────────────────────────────────────────────

  private tick(): void {
    const now = Date.now();
    this.detectFailedNodes(now);
    this.evictFromDrainingNodes();
    this.finalizeTerminating(now);
    this.restartBackedOffReplicas(now);
    this.resendStuckStarts(now);
    this.reconcileDeployments();
    this.db.deleteTerminatedReplicasBefore(now - TERMINATED_GC_MS);
  }

  // ── Node failure detection ───────────────────────────────────────────────

  private detectFailedNodes(now: number): void {
    if (now - this.startedAt < this.options.startupGraceMs) return;

    for (const node of this.db.getAllNodes()) {
      if (node.status !== 'Ready') continue;

      if (now - node.lastHeartbeatAt > NODE_TIMEOUT_MS) {
        log.warn('Node heartbeat timeout, marking NotReady', { node: node.name });
        this.db.updateNodeStatus(node.name, 'NotReady');
        this.db.addEvent('NodeNotReady', `node/${node.name}`,
          `Node missed ${HEARTBEAT_MISS_THRESHOLD} heartbeats`);

        // Commands queued for a dead node are now meaningless.
        this.pendingCommands.delete(node.name);
        this.rescheduleFromNode(node.name);
      }
    }
  }

  private rescheduleFromNode(nodeName: string): void {
    for (const replica of this.db.getReplicasOnNode(nodeName)) {
      if (replica.state === 'Terminated') continue;

      if (replica.state === 'Terminating') {
        // Nothing left to drain on a dead node.
        this.finalizeTermination(replica);
        continue;
      }

      log.info('Rescheduling replica off failed node', { replicaId: replica.id, fromNode: nodeName });
      this.scheduler.release(replica);
      replica.node = null;
      replica.state = 'Pending';
      replica.ready = false;
      replica.hostPort = 0;
      replica.nextRestartAt = undefined;
      replica.lastStateChange = Date.now();
      this.db.updateReplica(replica);

      this.db.addEvent('Rescheduled', `replica/${replica.id}`,
        `Rescheduling off failed node ${nodeName}`);
    }
  }

  // ── Drain ────────────────────────────────────────────────────────────────

  private evictFromDrainingNodes(): void {
    for (const node of this.db.getAllNodes()) {
      if (node.status !== 'Draining') continue;
      for (const replica of this.db.getReplicasOnNode(node.name)) {
        if (replica.state === 'Terminated' || replica.state === 'Terminating') continue;
        log.info('Evicting replica from draining node', { replicaId: replica.id, node: node.name });
        // The deployment's count reconciliation (same tick) creates a
        // replacement, which the scheduler places on a non-draining node.
        this.terminateReplica(replica, `node ${node.name} is draining`);
      }
    }
  }

  // ── Termination ──────────────────────────────────────────────────────────

  /**
   * Begin terminating a replica. A replica that was serving traffic is first
   * marked not-ready and kept running for TERMINATION_DRAIN_MS so load
   * balancers stop sending it requests. Others are stopped right away.
   */
  private terminateReplica(replica: Replica, reason: string): void {
    if (replica.state === 'Terminated' || replica.state === 'Terminating') return;

    if (replica.ready && replica.node) {
      replica.state = 'Terminating';
      replica.ready = false;
      replica.lastStateChange = Date.now();
      this.db.updateReplica(replica);
      this.db.addEvent('Terminated', `replica/${replica.id}`,
        `Replica terminating (${reason}); removed from load balancing, stopping in ${TERMINATION_DRAIN_MS} ms`);
      return;
    }

    this.db.addEvent('Terminated', `replica/${replica.id}`,
      `Replica terminating (${reason}) on node ${replica.node || 'unscheduled'}`);
    this.finalizeTermination(replica);
  }

  private finalizeTerminating(now: number): void {
    for (const replica of this.db.getReplicasByState('Terminating')) {
      if (now - replica.lastStateChange >= TERMINATION_DRAIN_MS) {
        this.finalizeTermination(replica);
      }
    }
  }

  private finalizeTermination(replica: Replica): void {
    if (replica.node) {
      this.sendStop(replica.node, replica.id, 30);
      this.scheduler.release(replica);
    }
    replica.state = 'Terminated';
    replica.ready = false;
    replica.lastStateChange = Date.now();
    this.db.updateReplica(replica);
  }

  // ── Crash restarts ───────────────────────────────────────────────────────

  private handleCrashedReplica(replica: Replica, exitCode: number, nodeName: string): void {
    replica.restartCount += 1;
    const backoff = Math.min(
      INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, replica.restartCount - 1),
      MAX_BACKOFF_MS
    );

    replica.state = 'CrashLoopBackOff';
    replica.ready = false;
    replica.nextRestartAt = Date.now() + backoff;
    replica.lastStateChange = Date.now();
    this.db.updateReplica(replica);

    this.db.addEvent('Crashed', `replica/${replica.id}`,
      `Container exited with code ${exitCode}, restart #${replica.restartCount} in ${backoff}ms`);
    log.warn('Container crashed, restart scheduled', {
      replicaId: replica.id,
      node: nodeName,
      exitCode,
      restartCount: replica.restartCount,
      backoffMs: backoff,
    });
  }

  /**
   * Restart replicas whose backoff has expired. The restart time is stored in
   * the database (not in a setTimeout), so it survives control plane restarts.
   */
  private restartBackedOffReplicas(now: number): void {
    for (const replica of this.db.getReplicasByState('CrashLoopBackOff')) {
      if (replica.nextRestartAt && now < replica.nextRestartAt) continue;

      if (!replica.node) {
        replica.state = 'Pending';
        replica.lastStateChange = now;
        this.db.updateReplica(replica);
        continue;
      }

      replica.generation += 1;
      replica.state = 'Scheduling';
      replica.nextRestartAt = undefined;
      replica.lastStateChange = now;
      this.db.updateReplica(replica);
      this.issueStart(replica);

      this.db.addEvent('Restarted', `replica/${replica.id}`,
        `Restarting container (attempt #${replica.restartCount})`);
    }
  }

  /**
   * A start command can be lost (agent disconnected, control plane restarted
   * before sending it). If the agent has not reported the replica after
   * START_RESEND_MS, send it again. Agents treat a repeated start for the same
   * generation as a no-op, so re-sending is always safe.
   */
  private resendStuckStarts(now: number): void {
    for (const replica of this.db.getReplicasByState('Scheduling')) {
      if (!replica.node || now - replica.lastStateChange < START_RESEND_MS) continue;
      log.warn('Replica not reported by agent, re-sending start', {
        replicaId: replica.id,
        node: replica.node,
      });
      replica.lastStateChange = now;
      this.db.updateReplica(replica);
      this.issueStart(replica);
    }
  }

  // ── Deployment reconciliation ────────────────────────────────────────────

  private reconcileDeployments(): void {
    for (const dep of this.db.getAllDeployments()) {
      const replicas = this.db.getReplicasForDeployment(dep.name);
      const activeReplicas = replicas.filter(
        r => r.state !== 'Terminated' && r.state !== 'Terminating'
      );
      const terminatingCount = replicas.filter(r => r.state === 'Terminating').length;

      const oldRevisionReplicas = activeReplicas.filter(r => r.revision !== dep.currentRevision);
      const newRevisionReplicas = activeReplicas.filter(r => r.revision === dep.currentRevision);

      if (oldRevisionReplicas.length > 0) {
        this.reconcileRollingUpdate(dep, oldRevisionReplicas, newRevisionReplicas, terminatingCount);
      } else {
        this.reconcileDesiredCount(dep, activeReplicas, terminatingCount);
      }

      for (const replica of this.db.getReplicasForDeployment(dep.name)) {
        if (replica.state === 'Pending') {
          this.scheduleReplica(replica);
        }
      }

      this.checkRolloutComplete(dep);
    }
  }

  private reconcileDesiredCount(dep: Deployment, activeReplicas: Replica[], terminatingCount: number): void {
    const diff = dep.desiredReplicas - activeReplicas.length;

    if (diff > 0) {
      // Replacements for replicas that are still shutting down (end of a
      // rollout, drain) must also respect the surge budget. For a plain
      // scale-up there is nothing terminating, so this never limits it.
      const budget = dep.desiredReplicas + dep.strategy.maxSurge - activeReplicas.length - terminatingCount;
      const toCreate = Math.min(diff, Math.max(0, budget));
      const spec = this.getSpec(dep.name, dep.currentRevision);
      if (!spec) return;
      for (let i = 0; i < toCreate; i++) {
        this.createReplica(dep, spec);
      }
    } else if (diff < 0) {
      // Too many replicas: remove excess, preferring ones that are not ready.
      const sorted = [...activeReplicas].sort((a, b) => {
        if (a.ready !== b.ready) return a.ready ? 1 : -1;
        return a.lastStateChange - b.lastStateChange;
      });
      for (let i = 0; i < -diff; i++) {
        this.terminateReplica(sorted[i], 'scale down');
      }
    }
  }

  // ── Rolling Updates ──────────────────────────────────────────────────────

  /**
   * Invariants (checked by tests):
   *   running containers (old + new + terminating) <= desired + maxSurge
   *   ready replicas                              >= desired - maxUnavailable
   */
  private reconcileRollingUpdate(
    dep: Deployment,
    oldReplicas: Replica[],
    newReplicas: Replica[],
    terminatingCount: number
  ): void {
    const totalDesired = dep.desiredReplicas;
    const { maxSurge, maxUnavailable } = dep.strategy;

    const newReady = newReplicas.filter(r => r.ready).length;
    const oldReady = oldReplicas.filter(r => r.ready);
    const oldNotReady = oldReplicas.filter(r => !r.ready);

    // Terminating replicas still have running containers, so they count
    // against the surge budget until they are actually stopped.
    const totalRunning = oldReplicas.length + newReplicas.length + terminatingCount;
    const canCreate = totalDesired + maxSurge - totalRunning;

    if (canCreate > 0 && newReplicas.length < totalDesired) {
      const spec = this.getSpec(dep.name, dep.currentRevision);
      if (spec) {
        const toCreate = Math.min(canCreate, totalDesired - newReplicas.length);
        for (let i = 0; i < toCreate; i++) {
          this.createReplica(dep, spec);
        }
      }
    }

    // Old replicas that are not ready serve no traffic: removing them never
    // reduces availability, so they go first and are not rate-limited.
    for (const r of oldNotReady) {
      this.terminateReplica(r, `rollout to revision ${dep.currentRevision}`);
    }

    // Ready old replicas are removed only while enough replicas stay ready.
    const minReady = totalDesired - maxUnavailable;
    const canTerminate = Math.max(0, newReady + oldReady.length - minReady);
    const toTerminate = Math.min(canTerminate, oldReady.length);
    for (let i = 0; i < toTerminate; i++) {
      this.terminateReplica(oldReady[i], `rollout to revision ${dep.currentRevision}`);
    }
  }

  private checkRolloutComplete(dep: Deployment): void {
    if (dep.completedRevision === dep.currentRevision) return;

    // Complete means every old-revision container is gone, including ones
    // still in their termination drain window.
    const live = this.db.getReplicasForDeployment(dep.name).filter(r => r.state !== 'Terminated');
    const hasOld = live.some(r => r.revision !== dep.currentRevision);
    const active = live.filter(r => r.state !== 'Terminating');
    const newReady = active.filter(r => r.revision === dep.currentRevision && r.ready).length;

    if (!hasOld && newReady >= dep.desiredReplicas) {
      this.db.markRolloutComplete(dep.name, dep.currentRevision);
      this.db.addEvent('RolloutCompleted', `deployment/${dep.name}`,
        `Revision ${dep.currentRevision} fully rolled out (${newReady}/${dep.desiredReplicas} ready)`);
      log.info('Rollout complete', { deployment: dep.name, revision: dep.currentRevision });
    }
  }

  // ── Replica lifecycle ────────────────────────────────────────────────────

  private getSpec(deployment: string, revision: number): DeploymentSpec | null {
    const rev = this.db.getRevision(deployment, revision);
    if (!rev) {
      log.error('Missing revision for deployment', { deployment, revision });
      return null;
    }
    return JSON.parse(rev.specJson) as DeploymentSpec;
  }

  private createReplica(dep: Deployment, spec: DeploymentSpec): void {
    const container = spec.spec.template.spec.containers[0];
    const replica: Replica = {
      id: `${dep.name}-${uuidv4().slice(0, 8)}`,
      deploymentName: dep.name,
      revision: dep.currentRevision,
      node: null,
      state: 'Pending',
      restartCount: 0,
      ready: false,
      containerPort: container.ports?.[0]?.containerPort || 8080,
      hostPort: 0, // assigned during scheduling
      lastStateChange: Date.now(),
      generation: 0,
      image: container.image,
      cpuRequest: parseCpuString(container.resources?.requests?.cpu),
      memRequest: parseMemoryString(container.resources?.requests?.memory),
    };

    this.db.insertReplica(replica);
    log.info('Created replica', { id: replica.id, deployment: dep.name, revision: replica.revision });
  }

  private scheduleReplica(replica: Replica): void {
    if (replica.nextRestartAt && Date.now() < replica.nextRestartAt) {
      return; // Still in backoff period
    }

    const result = this.scheduler.schedule(replica);

    if (result.success && result.nodeName) {
      replica.node = result.nodeName;
      replica.state = 'Scheduling';
      replica.hostPort = this.allocateHostPort(result.nodeName);
      replica.generation += 1;
      replica.nextRestartAt = undefined;
      replica.lastStateChange = Date.now();
      this.db.updateReplica(replica);
      this.issueStart(replica);

      this.db.addEvent('Scheduled', `replica/${replica.id}`,
        `Scheduled on node ${result.nodeName} (port ${replica.hostPort})`);
    } else {
      log.debug('Replica remains pending', { replicaId: replica.id, reason: result.reason });
    }
  }

  private issueStart(replica: Replica): void {
    if (!replica.node) return;
    const spec = this.getSpec(replica.deploymentName, replica.revision);
    const container = spec?.spec.template.spec.containers[0];
    const probe = container?.readinessProbe || container?.livenessProbe;

    this.enqueueCommand(replica.node, {
      kind: 'start',
      start: {
        replicaId: replica.id,
        image: replica.image,
        cpuRequest: replica.cpuRequest,
        memRequest: replica.memRequest,
        ports: [{ containerPort: replica.containerPort, hostPort: replica.hostPort }],
        env: (container?.env || []).map(e => ({ key: e.name, value: e.value })),
        healthPath: probe?.httpGet?.path || '/healthz',
        healthPort: probe?.httpGet?.port || replica.containerPort,
        healthIntervalMs: (probe?.periodSeconds || 5) * 1000,
        generation: replica.generation,
        args: container?.args || [],
      },
    });
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private sendStop(nodeName: string, replicaId: string, graceSeconds: number): void {
    this.lastStopSent.set(`${nodeName}/${replicaId}`, Date.now());
    this.enqueueCommand(nodeName, { kind: 'stop', stop: { replicaId, graceSeconds } });
  }

  /** Send a stop, but at most once per STOP_RESEND_MS for the same container. */
  private stopOnNode(nodeName: string, replicaId: string, graceSeconds: number, reason: string): void {
    const key = `${nodeName}/${replicaId}`;
    const last = this.lastStopSent.get(key) || 0;
    if (Date.now() - last < STOP_RESEND_MS) return;
    log.warn('Stopping container', { node: nodeName, replicaId, reason });
    this.sendStop(nodeName, replicaId, graceSeconds);
  }

  private enqueueCommand(nodeName: string, cmd: GrpcCommand): void {
    if (!this.pendingCommands.has(nodeName)) {
      this.pendingCommands.set(nodeName, []);
    }
    this.pendingCommands.get(nodeName)!.push(cmd);
  }

  /** Allocate a host port in the NodePort range (30000–32767) */
  private allocateHostPort(nodeName: string): number {
    const usedPorts = new Set(
      this.db.getReplicasOnNode(nodeName)
        .filter(r => r.state !== 'Terminated')
        .map(r => r.hostPort)
    );

    for (let port = 30000; port <= 32767; port++) {
      if (!usedPorts.has(port)) return port;
    }

    log.error('Port exhaustion on node', { node: nodeName });
    return 30000;
  }
}
