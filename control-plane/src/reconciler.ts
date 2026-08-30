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
// Unlike event-triggered systems, a level-triggered loop is self-healing:
// if a command is lost or the control plane restarts, the next tick will
// re-discover the drift and fix it.
//
// The loop handles:
// 1. Creating missing replicas for under-provisioned deployments
// 2. Removing excess replicas for over-provisioned deployments
// 3. Scheduling pending replicas onto nodes
// 4. Detecting crashed containers and restarting with exponential backoff
// 5. Rescheduling replicas off NotReady nodes
// 6. Rolling updates when a new revision is deployed
// ─────────────────────────────────────────────────────────────────────────────

/** Exponential backoff constants */
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;
const BACKOFF_MULTIPLIER = 2;

/** Heartbeat failure detection */
const HEARTBEAT_INTERVAL_MS = 1000;
const HEARTBEAT_MISS_THRESHOLD = 3;
const NODE_TIMEOUT_MS = HEARTBEAT_INTERVAL_MS * HEARTBEAT_MISS_THRESHOLD;

export class Reconciler {
  private pendingCommands: Map<string, GrpcCommand[]> = new Map(); // nodeName -> commands
  private running = false;
  private tickInterval: ReturnType<typeof setInterval> | null = null;

  constructor(
    private db: StateDB,
    private scheduler: Scheduler
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    log.info('Reconcile loop started');

    this.tickInterval = setInterval(() => {
      try {
        this.tick();
      } catch (err) {
        log.error('Reconcile tick error', { error: (err as Error).message });
      }
    }, 1000);
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

  /**
   * Process a heartbeat from an agent: update node timestamp and replica states.
   */
  processHeartbeat(
    nodeName: string,
    containers: GrpcContainerStatus[],
    used: { cpuUsed: number; memUsed: number }
  ): void {
    // Update node heartbeat
    this.db.updateNodeHeartbeat(nodeName, Date.now());

    // Update replica states from container reports
    for (const c of containers) {
      const replica = this.db.getReplica(c.replicaId);
      if (!replica) {
        // Orphaned container — schedule it for stopping
        log.warn('Orphaned container detected, stopping', {
          replicaId: c.replicaId,
          node: nodeName,
        });
        this.enqueueCommand(nodeName, {
          kind: 'stop',
          stop: { replicaId: c.replicaId, graceSeconds: 10 },
        });
        continue;
      }

      // Update state based on container report
      if (c.state === 'running' && c.healthCheck) {
        if (replica.state !== 'Ready') {
          replica.state = 'Ready';
          replica.ready = true;
          replica.lastStateChange = Date.now();
          this.db.updateReplica(replica);
          this.db.addEvent('Ready', `replica/${replica.id}`,
            `Replica is ready on node ${nodeName}`);
        }
      } else if (c.state === 'running' && !c.healthCheck) {
        if (replica.state !== 'Unhealthy' && replica.state !== 'Starting') {
          replica.state = 'Unhealthy';
          replica.ready = false;
          replica.lastStateChange = Date.now();
          this.db.updateReplica(replica);
          this.db.addEvent('Unhealthy', `replica/${replica.id}`,
            `Replica failed health check on node ${nodeName}`);
        }
      } else if (c.state === 'exited') {
        this.handleCrashedReplica(replica, c.exitCode);
      } else if (c.state === 'starting') {
        if (replica.state !== 'Starting') {
          replica.state = 'Starting';
          replica.lastStateChange = Date.now();
          this.db.updateReplica(replica);
        }
      }
    }
  }

  // ── Main reconcile tick ──────────────────────────────────────────────────

  private tick(): void {
    this.detectFailedNodes();
    this.reconcileDeployments();
  }

  // ── Node failure detection ───────────────────────────────────────────────

  private detectFailedNodes(): void {
    const now = Date.now();
    const nodes = this.db.getAllNodes();

    for (const node of nodes) {
      if (node.status === 'NotReady') continue;
      if (node.status === 'Draining') continue;

      if (now - node.lastHeartbeatAt > NODE_TIMEOUT_MS) {
        log.warn('Node heartbeat timeout, marking NotReady', { node: node.name });
        this.db.updateNodeStatus(node.name, 'NotReady');
        this.db.addEvent('NodeNotReady', `node/${node.name}`,
          `Node missed ${HEARTBEAT_MISS_THRESHOLD} heartbeats`);

        // Reschedule replicas off the failed node
        this.rescheduleFromNode(node.name);
      }
    }
  }

  // ── Deployment reconciliation ────────────────────────────────────────────

  private reconcileDeployments(): void {
    const deployments = this.db.getAllDeployments();

    for (const dep of deployments) {
      const replicas = this.db.getReplicasForDeployment(dep.name);
      const activeReplicas = replicas.filter(
        r => !['Terminated', 'Terminating'].includes(r.state)
      );

      // Handle rolling updates: check if all replicas are on the current revision
      const oldRevisionReplicas = activeReplicas.filter(r => r.revision !== dep.currentRevision);
      const newRevisionReplicas = activeReplicas.filter(r => r.revision === dep.currentRevision);

      if (oldRevisionReplicas.length > 0) {
        this.reconcileRollingUpdate(dep, oldRevisionReplicas, newRevisionReplicas);
      } else {
        this.reconcileDesiredCount(dep, activeReplicas);
      }

      // Schedule pending replicas
      for (const replica of this.db.getReplicasForDeployment(dep.name)) {
        if (replica.state === 'Pending') {
          this.scheduleReplica(replica);
        }
      }
    }
  }

  private reconcileDesiredCount(dep: Deployment, activeReplicas: Replica[]): void {
    const diff = dep.desiredReplicas - activeReplicas.length;

    if (diff > 0) {
      // Need more replicas
      const revision = this.db.getRevision(dep.name, dep.currentRevision);
      if (!revision) {
        log.error('Missing revision for deployment', {
          deployment: dep.name,
          revision: dep.currentRevision,
        });
        return;
      }

      const spec = JSON.parse(revision.specJson) as DeploymentSpec;
      for (let i = 0; i < diff; i++) {
        this.createReplica(dep, spec);
      }
    } else if (diff < 0) {
      // Too many replicas — remove excess (prefer non-ready ones)
      const sorted = [...activeReplicas].sort((a, b) => {
        if (a.ready !== b.ready) return a.ready ? 1 : -1;
        return a.lastStateChange - b.lastStateChange;
      });

      for (let i = 0; i < -diff; i++) {
        this.terminateReplica(sorted[i]);
      }
    }
  }

  // ── Rolling Updates ──────────────────────────────────────────────────────

  private reconcileRollingUpdate(
    dep: Deployment,
    oldReplicas: Replica[],
    newReplicas: Replica[]
  ): void {
    const totalDesired = dep.desiredReplicas;
    const maxSurge = dep.strategy.maxSurge;
    const maxUnavailable = dep.strategy.maxUnavailable;

    // Count states
    const newReady = newReplicas.filter(r => r.ready).length;
    const newTotal = newReplicas.length;
    const oldTotal = oldReplicas.length;
    const totalActive = newTotal + oldTotal;

    // How many new replicas can we create? (respecting maxSurge)
    const maxTotal = totalDesired + maxSurge;
    const canCreate = maxTotal - totalActive;

    // How many old replicas can we terminate? (respecting maxUnavailable)
    const readyCount = newReady + oldReplicas.filter(r => r.ready).length;
    const minReady = totalDesired - maxUnavailable;
    const canTerminate = Math.max(0, readyCount - minReady);

    // Create new replicas if we have room
    if (canCreate > 0 && newTotal < totalDesired) {
      const toCreate = Math.min(canCreate, totalDesired - newTotal);
      const revision = this.db.getRevision(dep.name, dep.currentRevision);
      if (revision) {
        const spec = JSON.parse(revision.specJson) as DeploymentSpec;
        for (let i = 0; i < toCreate; i++) {
          this.createReplica(dep, spec);
        }
      }
    }

    // Terminate old replicas if new ones are ready
    if (canTerminate > 0 && oldTotal > 0) {
      const toTerminate = Math.min(canTerminate, oldTotal);
      for (let i = 0; i < toTerminate; i++) {
        this.terminateReplica(oldReplicas[i]);
      }
    }

    // Check if rollout is complete
    if (oldTotal === 0 && newReady >= totalDesired) {
      this.db.addEvent('RolloutCompleted', `deployment/${dep.name}`,
        `Rolling update to revision ${dep.currentRevision} complete`);
    }
  }

  // ── Replica lifecycle ────────────────────────────────────────────────────

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
      image: container.image,
      cpuRequest: parseCpuString(container.resources?.requests?.cpu),
      memRequest: parseMemoryString(container.resources?.requests?.memory),
    };

    this.db.insertReplica(replica);
    log.info('Created replica', { id: replica.id, deployment: dep.name });
  }

  private scheduleReplica(replica: Replica): void {
    // Check backoff
    if (replica.nextRestartAt && Date.now() < replica.nextRestartAt) {
      return; // Still in backoff period
    }

    const result = this.scheduler.schedule(replica);

    if (result.success && result.nodeName) {
      replica.node = result.nodeName;
      replica.state = 'Scheduling';
      replica.hostPort = this.allocateHostPort(result.nodeName);
      replica.lastStateChange = Date.now();
      this.db.updateReplica(replica);

      // Get the spec for health check configuration
      const revision = this.db.getRevision(replica.deploymentName, replica.revision);
      const spec = revision ? JSON.parse(revision.specJson) as DeploymentSpec : null;
      const container = spec?.spec.template.spec.containers[0];
      const probe = container?.readinessProbe || container?.livenessProbe;

      this.enqueueCommand(result.nodeName, {
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
        },
      });

      this.db.addEvent('Scheduled', `replica/${replica.id}`,
        `Scheduled on node ${result.nodeName} (port ${replica.hostPort})`);
    } else {
      // Stay pending with reason
      log.debug('Replica remains pending', {
        replicaId: replica.id,
        reason: result.reason,
      });
    }
  }

  private terminateReplica(replica: Replica): void {
    if (replica.state === 'Terminated' || replica.state === 'Terminating') return;

    replica.state = 'Terminating';
    replica.ready = false;
    replica.lastStateChange = Date.now();
    this.db.updateReplica(replica);

    if (replica.node) {
      this.enqueueCommand(replica.node, {
        kind: 'stop',
        stop: { replicaId: replica.id, graceSeconds: 30 },
      });
      this.scheduler.release(replica);
    }

    this.db.addEvent('Terminated', `replica/${replica.id}`,
      `Replica terminating on node ${replica.node || 'unscheduled'}`);

    // Mark as fully terminated after issuing the stop command
    // The agent will confirm; for now we clean up the DB state
    replica.state = 'Terminated';
    this.db.updateReplica(replica);
  }

  private handleCrashedReplica(replica: Replica, exitCode: number): void {
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

    log.warn('Container crashed, scheduling restart', {
      replicaId: replica.id,
      exitCode,
      restartCount: replica.restartCount,
      backoffMs: backoff,
    });

    // After backoff, move to Pending for rescheduling on same node
    setTimeout(() => {
      const current = this.db.getReplica(replica.id);
      if (current && current.state === 'CrashLoopBackOff') {
        current.state = 'Scheduling';
        current.lastStateChange = Date.now();
        this.db.updateReplica(current);

        // Re-issue start command to the node
        if (current.node) {
          const revision = this.db.getRevision(current.deploymentName, current.revision);
          const spec = revision ? JSON.parse(revision.specJson) as DeploymentSpec : null;
          const container = spec?.spec.template.spec.containers[0];
          const probe = container?.readinessProbe || container?.livenessProbe;

          this.enqueueCommand(current.node, {
            kind: 'start',
            start: {
              replicaId: current.id,
              image: current.image,
              cpuRequest: current.cpuRequest,
              memRequest: current.memRequest,
              ports: [{ containerPort: current.containerPort, hostPort: current.hostPort }],
              env: (container?.env || []).map(e => ({ key: e.name, value: e.value })),
              healthPath: probe?.httpGet?.path || '/healthz',
              healthPort: probe?.httpGet?.port || current.containerPort,
              healthIntervalMs: (probe?.periodSeconds || 5) * 1000,
            },
          });

          this.db.addEvent('Restarted', `replica/${current.id}`,
            `Restarting container (attempt #${current.restartCount})`);
        }
      }
    }, backoff);
  }

  // ── Node rescheduling ────────────────────────────────────────────────────

  private rescheduleFromNode(nodeName: string): void {
    const replicas = this.db.getReplicasOnNode(nodeName);
    for (const replica of replicas) {
      if (['Terminated', 'Terminating'].includes(replica.state)) continue;

      log.info('Rescheduling replica off failed node', {
        replicaId: replica.id,
        fromNode: nodeName,
      });

      // Release resources and mark as pending for rescheduling
      this.scheduler.release(replica);
      replica.node = null;
      replica.state = 'Pending';
      replica.ready = false;
      replica.hostPort = 0;
      replica.lastStateChange = Date.now();
      this.db.updateReplica(replica);

      this.db.addEvent('Rescheduled', `replica/${replica.id}`,
        `Rescheduling off failed node ${nodeName}`);
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

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

    // Fallback: this shouldn't happen in practice
    log.error('Port exhaustion on node', { node: nodeName });
    return 30000;
  }
}
