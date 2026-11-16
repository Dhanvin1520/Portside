import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import { createLogger, GrpcContainerStatus } from '@portside/shared';

const log = createLogger('agent');

// ─────────────────────────────────────────────────────────────────────────────
// Worker Agent
//
// The agent is intentionally "dumb": it only manages local containers.
// All scheduling and orchestration decisions are made by the control plane.
//
// Lifecycle (repeated forever until stop()):
// 1. Register with the control plane (node name + resources)
// 2. Open a bidirectional gRPC stream
// 3. Send a heartbeat every second with the status of every container
// 4. Execute start/stop commands received from the stream
// 5. If the stream breaks, keep the containers running and reconnect with
//    exponential backoff + jitter. Losing the control plane must never take
//    the workloads down.
// ─────────────────────────────────────────────────────────────────────────────

export interface StartParams {
  replicaId: string;
  image: string;
  cpuRequest: number;
  memRequest: number;
  ports: { containerPort: number; hostPort: number }[];
  env: { key: string; value: string }[];
  args: string[];
  healthPath: string;
  healthPort: number;
  healthIntervalMs: number;
  generation: number;
}

/** What the agent needs from a container runtime (Docker in production). */
export interface ContainerRuntime {
  startContainer(params: StartParams): Promise<void>;
  stopContainer(replicaId: string, graceSeconds: number): Promise<void>;
  getContainerStatuses(): Promise<GrpcContainerStatus[]>;
  getResourceUsage(): { cpuUsed: number; memUsed: number };
  /** Rebuild in-memory state from containers that already exist (agent restart). */
  adoptExisting?(): Promise<void>;
}

export interface AgentOptions {
  nodeName: string;
  controlPlane: string;
  cpuCapacity: number;
  memCapacity: number;
  heartbeatIntervalMs?: number;
  reconnectInitialMs?: number;
  reconnectMaxMs?: number;
  protoPath?: string;
}

const DEFAULT_PROTO_PATH = path.resolve(__dirname, '../../proto/agent.proto');

export class Agent {
  private proto: any;
  private client: any = null;
  private stream: any = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private stopped = false;
  private loop: Promise<void> | null = null;
  private wakeUp: (() => void) | null = null;
  /** Commands for the same replica run strictly in order. */
  private replicaQueues: Map<string, Promise<void>> = new Map();
  private readonly opts: Required<AgentOptions>;

  constructor(options: AgentOptions, private runtime: ContainerRuntime) {
    this.opts = {
      heartbeatIntervalMs: 1000,
      reconnectInitialMs: 250,
      reconnectMaxMs: 2000,
      protoPath: DEFAULT_PROTO_PATH,
      ...options,
    };
    const packageDef = protoLoader.loadSync(this.opts.protoPath, {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });
    this.proto = grpc.loadPackageDefinition(packageDef) as any;
  }

  async start(): Promise<void> {
    if (this.runtime.adoptExisting) {
      await this.runtime.adoptExisting();
    }
    this.loop = this.connectLoop();
  }

  /**
   * Stop the agent. By default the containers are left running (this is how
   * tests simulate a node that crashes or is partitioned away).
   */
  async stop(options: { stopContainers?: boolean } = {}): Promise<void> {
    this.stopped = true;
    this.wakeUp?.();
    this.teardownConnection(true);
    await this.loop;
    if (options.stopContainers) {
      const statuses = await this.runtime.getContainerStatuses();
      for (const s of statuses) {
        await this.runtime.stopContainer(s.replicaId, 5);
      }
    }
  }

  // ── Connection management ────────────────────────────────────────────────

  private async connectLoop(): Promise<void> {
    let attempt = 0;
    while (!this.stopped) {
      try {
        this.client = new this.proto.portside.ControlPlane(
          this.opts.controlPlane,
          grpc.credentials.createInsecure(),
          {
            'grpc.initial_reconnect_backoff_ms': this.opts.reconnectInitialMs,
            'grpc.max_reconnect_backoff_ms': this.opts.reconnectMaxMs,
            // Detect half-open connections (e.g. network partition) quickly.
            'grpc.keepalive_time_ms': 5000,
            'grpc.keepalive_timeout_ms': 3000,
            'grpc.keepalive_permit_without_calls': 1,
          }
        );
        await this.register();
        attempt = 0;
        await this.runStream();
      } catch (err) {
        if (!this.stopped) {
          log.warn('Control plane connection failed', { error: (err as Error).message });
        }
      } finally {
        this.teardownConnection(false);
      }

      if (this.stopped) break;
      const base = Math.min(this.opts.reconnectMaxMs, this.opts.reconnectInitialMs * 2 ** attempt);
      const delay = Math.round(base * (0.5 + Math.random() * 0.5)); // jitter
      attempt++;
      log.info('Reconnecting to control plane', { inMs: delay, attempt });
      await new Promise<void>(resolve => {
        const t = setTimeout(resolve, delay);
        this.wakeUp = () => { clearTimeout(t); resolve(); };
      });
      this.wakeUp = null;
    }
  }

  private register(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.client.Register(
        {
          nodeName: this.opts.nodeName,
          cpuCapacity: this.opts.cpuCapacity,
          memCapacity: this.opts.memCapacity,
        },
        { deadline: Date.now() + 3000 },
        (err: Error | null, response: any) => {
          if (err) return reject(err);
          if (response && response.accepted === false) {
            return reject(new Error(`Registration rejected: ${response.message}`));
          }
          log.info('Registered with control plane', { node: this.opts.nodeName });
          resolve();
        }
      );
    });
  }

  /** Resolves when the heartbeat stream ends for any reason. */
  private runStream(): Promise<void> {
    return new Promise(resolve => {
      const stream = this.client.Heartbeat();
      this.stream = stream;
      let done = false;
      const finish = (why: string, err?: Error) => {
        if (done) return;
        done = true;
        if (!this.stopped) log.warn(`Heartbeat stream ${why}`, { error: err?.message });
        resolve();
      };

      stream.on('data', (command: any) => this.handleCommand(command));
      stream.on('error', (err: Error) => finish('error', err));
      stream.on('end', () => finish('ended by server'));
      stream.on('close', () => finish('closed'));

      let inFlight = false;
      const beat = async () => {
        if (inFlight || done) return; // never overlap heartbeats
        inFlight = true;
        try {
          const containers = await this.runtime.getContainerStatuses();
          const used = this.runtime.getResourceUsage();
          if (!done) {
            stream.write({
              nodeName: this.opts.nodeName,
              containers,
              used: { cpuUsed: used.cpuUsed, memUsed: used.memUsed },
            });
          }
        } catch (err) {
          log.error('Heartbeat send error', { error: (err as Error).message });
        } finally {
          inFlight = false;
        }
      };

      this.heartbeatTimer = setInterval(beat, this.opts.heartbeatIntervalMs);
      void beat(); // report immediately on (re)connect
      log.info('Heartbeat stream open', { node: this.opts.nodeName });
    });
  }

  private teardownConnection(cancel: boolean): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.stream) {
      try {
        if (cancel) this.stream.cancel(); else this.stream.end();
      } catch { /* already closed */ }
      this.stream = null;
    }
    if (this.client) {
      try { this.client.close(); } catch { /* ignore */ }
      this.client = null;
    }
  }

  // ── Commands ─────────────────────────────────────────────────────────────

  private handleCommand(command: any): void {
    if (command.start) {
      const s = command.start;
      const params: StartParams = {
        replicaId: s.replicaId,
        image: s.image,
        cpuRequest: s.cpuRequest || 100,
        memRequest: s.memRequest || 128,
        ports: (s.ports || []).map((p: any) => ({ containerPort: p.containerPort, hostPort: p.hostPort })),
        env: (s.env || []).map((e: any) => ({ key: e.key, value: e.value })),
        args: s.args || [],
        healthPath: s.healthPath || '/healthz',
        healthPort: s.healthPort || 8080,
        healthIntervalMs: s.healthIntervalMs || 5000,
        generation: s.generation || 0,
      };
      log.info('Received start command', { replicaId: params.replicaId, generation: params.generation });
      this.enqueue(params.replicaId, () => this.runtime.startContainer(params));
    }

    if (command.stop) {
      const { replicaId, graceSeconds } = command.stop;
      log.info('Received stop command', { replicaId });
      this.enqueue(replicaId, () => this.runtime.stopContainer(replicaId, graceSeconds || 30));
    }
  }

  private enqueue(replicaId: string, task: () => Promise<void>): void {
    const prev = this.replicaQueues.get(replicaId) || Promise.resolve();
    const next = prev
      .then(task)
      .catch(err => { log.error('Command failed', { replicaId, error: (err as Error).message }); })
      .finally(() => {
        if (this.replicaQueues.get(replicaId) === next) this.replicaQueues.delete(replicaId);
      });
    this.replicaQueues.set(replicaId, next);
  }
}
