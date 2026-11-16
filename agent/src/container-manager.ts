import Docker from 'dockerode';
import http from 'http';
import { createLogger, GrpcContainerStatus } from '@portside/shared';
import { ContainerRuntime, StartParams } from './agent';

const log = createLogger('container-manager');

// ─────────────────────────────────────────────────────────────────────────────
// Container Manager (Docker runtime)
//
// Manages Docker containers on a worker node via dockerode.
// Each container carries labels with its replica ID, placement generation and
// health-check settings, so a restarted agent can rebuild its state from
// Docker alone (adoptExisting) instead of losing track of running workloads.
// Health checks are performed via HTTP GET requests.
// ─────────────────────────────────────────────────────────────────────────────

const LABEL = {
  managed: 'portside.managed',
  replicaId: 'portside.replica-id',
  generation: 'portside.generation',
  hostPort: 'portside.host-port',
  containerPort: 'portside.container-port',
  healthPath: 'portside.health-path',
  healthPort: 'portside.health-port',
  healthInterval: 'portside.health-interval-ms',
};

interface ManagedContainer {
  replicaId: string;
  containerId: string;
  generation: number;
  containerPort: number;
  hostPort: number;
  healthPath: string;
  healthPort: number;
  healthIntervalMs: number;
  healthCheckTimer?: ReturnType<typeof setInterval>;
  healthy: boolean;
}

export class ContainerManager implements ContainerRuntime {
  private docker: Docker;
  private containers: Map<string, ManagedContainer> = new Map();
  /** Replicas whose start is in progress (image pull can take a while). */
  private starting: Map<string, number> = new Map(); // replicaId -> generation

  constructor() {
    this.docker = new Docker({ socketPath: '/var/run/docker.sock' });
  }

  /**
   * Rebuild the in-memory map from labeled containers that already exist.
   * Called once at agent startup.
   */
  async adoptExisting(): Promise<void> {
    const list = await this.docker.listContainers({
      all: true,
      filters: { label: [`${LABEL.managed}=true`] },
    });
    for (const c of list) {
      const l = c.Labels;
      const replicaId = l[LABEL.replicaId];
      if (!replicaId) continue;
      const managed: ManagedContainer = {
        replicaId,
        containerId: c.Id,
        generation: parseInt(l[LABEL.generation] || '0', 10),
        containerPort: parseInt(l[LABEL.containerPort] || '0', 10),
        hostPort: parseInt(l[LABEL.hostPort] || '0', 10),
        healthPath: l[LABEL.healthPath] || '/healthz',
        healthPort: parseInt(l[LABEL.healthPort] || '0', 10),
        healthIntervalMs: parseInt(l[LABEL.healthInterval] || '5000', 10),
        healthy: false,
      };
      this.containers.set(replicaId, managed);
      if (c.State === 'running') this.startHealthChecks(managed);
      log.info('Adopted existing container', { replicaId, state: c.State, generation: managed.generation });
    }
  }

  async startContainer(params: StartParams): Promise<void> {
    const { replicaId, image, generation } = params;

    // Idempotency: a repeated start for the same generation is a no-op.
    if (this.starting.get(replicaId) === generation) return;
    const existing = this.containers.get(replicaId);
    if (existing && existing.generation === generation && await this.isRunning(existing.containerId)) {
      log.info('Start ignored, container already running', { replicaId, generation });
      return;
    }

    this.starting.set(replicaId, generation);
    try {
      // Replace any previous instance of this replica (crash restart).
      if (existing) await this.stopContainer(replicaId, 5);
      await this.removeByName(`portside-${replicaId}`);

      await this.ensureImage(image);

      const port = params.ports[0] || { containerPort: 8080, hostPort: 30000 };
      const healthPort = params.healthPort || port.containerPort;
      // The health check runs from the agent against the published host port.
      const envVars = params.env.map(e => `${e.key}=${e.value}`);
      envVars.push(`PORTSIDE_REPLICA_ID=${replicaId}`);

      log.info('Starting container', { replicaId, image, generation });
      const container = await this.docker.createContainer({
        Image: image,
        name: `portside-${replicaId}`,
        Cmd: params.args.length > 0 ? params.args : undefined,
        Env: envVars,
        Labels: {
          [LABEL.managed]: 'true',
          [LABEL.replicaId]: replicaId,
          [LABEL.generation]: String(generation),
          [LABEL.hostPort]: String(port.hostPort),
          [LABEL.containerPort]: String(port.containerPort),
          [LABEL.healthPath]: params.healthPath,
          [LABEL.healthPort]: String(healthPort),
          [LABEL.healthInterval]: String(params.healthIntervalMs),
        },
        ExposedPorts: { [`${port.containerPort}/tcp`]: {} },
        HostConfig: {
          PortBindings: {
            [`${port.containerPort}/tcp`]: [{ HostPort: String(port.hostPort) }],
          },
          Memory: params.memRequest * 1024 * 1024, // MiB → bytes
          NanoCpus: params.cpuRequest * 1_000_000,  // millicores → nanocpus
          RestartPolicy: { Name: '', MaximumRetryCount: 0 }, // the control plane owns restarts
        },
      });

      await container.start();

      const managed: ManagedContainer = {
        replicaId,
        containerId: container.id,
        generation,
        containerPort: port.containerPort,
        hostPort: port.hostPort,
        healthPath: params.healthPath,
        healthPort,
        healthIntervalMs: params.healthIntervalMs || 5000,
        healthy: false,
      };
      this.containers.set(replicaId, managed);
      this.startHealthChecks(managed);
      log.info('Container started', { replicaId, containerId: container.id.slice(0, 12), generation });
    } finally {
      if (this.starting.get(replicaId) === generation) this.starting.delete(replicaId);
    }
  }

  async stopContainer(replicaId: string, graceSeconds: number): Promise<void> {
    const managed = this.containers.get(replicaId);
    if (managed?.healthCheckTimer) clearInterval(managed.healthCheckTimer);
    this.containers.delete(replicaId);

    log.info('Stopping container', { replicaId, graceSeconds });
    // Stop by name too, so containers this process never tracked (left over
    // from before a restart, or a stale copy after a partition) are removed.
    const targets = new Set<string>();
    if (managed) targets.add(managed.containerId);
    targets.add(`portside-${replicaId}`);
    for (const id of targets) {
      const c = this.docker.getContainer(id);
      await c.stop({ t: graceSeconds }).catch(() => {});
      await c.remove({ force: true }).catch(() => {});
    }
  }

  /** Status of every managed container, queried from Docker. */
  async getContainerStatuses(): Promise<GrpcContainerStatus[]> {
    const statuses: GrpcContainerStatus[] = [];

    for (const [replicaId, managed] of this.containers) {
      try {
        const info = await this.docker.getContainer(managed.containerId).inspect();
        let state = 'running';
        let exitCode = 0;
        if (info.State.Running) {
          state = 'running';
        } else if (info.State.Restarting) {
          state = 'starting';
        } else {
          state = 'exited';
          exitCode = info.State.ExitCode;
        }
        statuses.push({
          replicaId,
          state,
          exitCode,
          healthCheck: state === 'running' && managed.healthy,
          restartCount: 0,
          generation: managed.generation,
        });
      } catch {
        // Container vanished (e.g. removed by hand): report it once as killed.
        statuses.push({
          replicaId, state: 'exited', exitCode: 137, healthCheck: false,
          restartCount: 0, generation: managed.generation,
        });
        if (managed.healthCheckTimer) clearInterval(managed.healthCheckTimer);
        this.containers.delete(replicaId);
      }
    }

    // Also report labeled containers we are not tracking, so the control
    // plane can tell us to stop them.
    try {
      const all = await this.docker.listContainers({
        all: true,
        filters: { label: [`${LABEL.managed}=true`] },
      });
      for (const c of all) {
        const replicaId = c.Labels[LABEL.replicaId];
        if (!replicaId || this.containers.has(replicaId) || this.starting.has(replicaId)) continue;
        statuses.push({
          replicaId,
          state: c.State === 'running' ? 'running' : 'exited',
          exitCode: 0,
          healthCheck: false,
          restartCount: 0,
          generation: parseInt(c.Labels[LABEL.generation] || '0', 10),
        });
      }
    } catch {
      // Docker query failed — skip orphan detection this round
    }

    return statuses;
  }

  getResourceUsage(): { cpuUsed: number; memUsed: number } {
    // Approximation; a real implementation would read `docker stats`.
    return {
      cpuUsed: this.containers.size * 100,
      memUsed: this.containers.size * 128,
    };
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  private startHealthChecks(managed: ManagedContainer): void {
    if (managed.healthCheckTimer) clearInterval(managed.healthCheckTimer);
    const check = async () => {
      managed.healthy = await this.checkHealth(managed);
    };
    // First check soon after start so readiness is not delayed a full period.
    setTimeout(check, Math.min(500, managed.healthIntervalMs));
    managed.healthCheckTimer = setInterval(check, managed.healthIntervalMs);
  }

  private async isRunning(containerId: string): Promise<boolean> {
    try {
      return (await this.docker.getContainer(containerId).inspect()).State.Running;
    } catch {
      return false;
    }
  }

  private async removeByName(name: string): Promise<void> {
    await this.docker.getContainer(name).remove({ force: true }).catch(() => {});
  }

  private async ensureImage(image: string): Promise<void> {
    try {
      await this.docker.getImage(image).inspect();
      return;
    } catch {
      log.info('Pulling image', { image });
    }
    await new Promise<void>((resolve, reject) => {
      this.docker.pull(image, (err: Error | null, stream: NodeJS.ReadableStream) => {
        if (err) return reject(err);
        this.docker.modem.followProgress(stream, (err2: Error | null) => {
          if (err2) return reject(err2);
          resolve();
        });
      });
    });
  }

  private checkHealth(managed: ManagedContainer): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const req = http.get(
        {
          hostname: '127.0.0.1',
          port: managed.hostPort,
          path: managed.healthPath,
          timeout: 2000,
        },
        (res) => {
          res.resume();
          resolve(res.statusCode !== undefined && res.statusCode >= 200 && res.statusCode < 400);
        }
      );
      req.on('error', () => resolve(false));
      req.on('timeout', () => {
        req.destroy();
        resolve(false);
      });
    });
  }
}
