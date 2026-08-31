import Docker from 'dockerode';
import http from 'http';
import { createLogger, GrpcContainerStatus } from '@portside/shared';

const log = createLogger('container-manager');

// ─────────────────────────────────────────────────────────────────────────────
// Container Manager
//
// Manages Docker containers on a worker node via dockerode.
// Each container is labeled with its replica ID for identification.
// Health checks are performed via HTTP GET requests.
// ─────────────────────────────────────────────────────────────────────────────

interface ManagedContainer {
  replicaId: string;
  containerId: string;
  image: string;
  containerPort: number;
  hostPort: number;
  healthPath: string;
  healthPort: number;
  healthIntervalMs: number;
  healthCheckTimer?: ReturnType<typeof setInterval>;
  healthy: boolean;
  restartCount: number;
}

export class ContainerManager {
  private docker: Docker;
  private containers: Map<string, ManagedContainer> = new Map();

  constructor() {
    this.docker = new Docker({ socketPath: '/var/run/docker.sock' });
  }

  async startContainer(params: {
    replicaId: string;
    image: string;
    cpuRequest: number;
    memRequest: number;
    ports: { containerPort: number; hostPort: number }[];
    env: { key: string; value: string }[];
    healthPath: string;
    healthPort: number;
    healthIntervalMs: number;
  }): Promise<void> {
    const { replicaId, image } = params;

    // Stop existing container if running (idempotent restart)
    if (this.containers.has(replicaId)) {
      await this.stopContainer(replicaId, 5);
    }

    log.info('Starting container', { replicaId, image });

    try {
      // Pull image if not available
      try {
        await this.docker.getImage(image).inspect();
      } catch {
        log.info('Pulling image', { image });
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

      const port = params.ports[0] || { containerPort: 8080, hostPort: 30000 };
      const envVars = params.env.map(e => `${e.key}=${e.value}`);
      envVars.push(`PORTSIDE_REPLICA_ID=${replicaId}`);

      const container = await this.docker.createContainer({
        Image: image,
        name: `portside-${replicaId}`,
        Env: envVars,
        Labels: {
          'portside.replica-id': replicaId,
          'portside.managed': 'true',
        },
        ExposedPorts: {
          [`${port.containerPort}/tcp`]: {},
        },
        HostConfig: {
          PortBindings: {
            [`${port.containerPort}/tcp`]: [
              { HostPort: String(port.hostPort) },
            ],
          },
          Memory: params.memRequest * 1024 * 1024, // MiB → bytes
          NanoCpus: params.cpuRequest * 1_000_000,  // millicores → nanocpus
          RestartPolicy: { Name: '', MaximumRetryCount: 0 }, // We handle restarts
        },
      });

      await container.start();

      const managed: ManagedContainer = {
        replicaId,
        containerId: container.id,
        image,
        containerPort: port.containerPort,
        hostPort: port.hostPort,
        healthPath: params.healthPath,
        healthPort: params.healthPort || port.containerPort,
        healthIntervalMs: params.healthIntervalMs || 5000,
        healthy: false,
        restartCount: 0,
      };

      // Start health checking
      managed.healthCheckTimer = setInterval(async () => {
        managed.healthy = await this.checkHealth(managed);
      }, managed.healthIntervalMs);

      this.containers.set(replicaId, managed);
      log.info('Container started', { replicaId, containerId: container.id.slice(0, 12) });
    } catch (err) {
      log.error('Failed to start container', {
        replicaId,
        error: (err as Error).message,
      });
      throw err;
    }
  }

  async stopContainer(replicaId: string, graceSeconds: number): Promise<void> {
    const managed = this.containers.get(replicaId);
    if (!managed) return;

    log.info('Stopping container', { replicaId, graceSeconds });

    if (managed.healthCheckTimer) {
      clearInterval(managed.healthCheckTimer);
    }

    try {
      const container = this.docker.getContainer(managed.containerId);
      await container.stop({ t: graceSeconds }).catch(() => {});
      await container.remove({ force: true }).catch(() => {});
    } catch (err) {
      log.warn('Error stopping container (may already be stopped)', {
        replicaId,
        error: (err as Error).message,
      });
    }

    this.containers.delete(replicaId);
  }

  /**
   * Get the status of all managed containers.
   * Queries Docker for actual container state.
   */
  async getContainerStatuses(): Promise<GrpcContainerStatus[]> {
    const statuses: GrpcContainerStatus[] = [];

    for (const [replicaId, managed] of this.containers) {
      try {
        const container = this.docker.getContainer(managed.containerId);
        const info = await container.inspect();

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
          healthCheck: managed.healthy,
          restartCount: managed.restartCount,
        });
      } catch {
        // Container gone — report as exited
        statuses.push({
          replicaId,
          state: 'exited',
          exitCode: 137,
          healthCheck: false,
          restartCount: managed.restartCount,
        });
        this.containers.delete(replicaId);
      }
    }

    // Also check for orphaned Portside containers
    try {
      const allContainers = await this.docker.listContainers({
        all: true,
        filters: { label: ['portside.managed=true'] },
      });

      for (const c of allContainers) {
        const replicaId = c.Labels['portside.replica-id'];
        if (replicaId && !this.containers.has(replicaId)) {
          statuses.push({
            replicaId,
            state: c.State === 'running' ? 'running' : 'exited',
            exitCode: 0,
            healthCheck: false,
            restartCount: 0,
          });
        }
      }
    } catch {
      // Docker query failed — skip orphan detection
    }

    return statuses;
  }

  // ── Health checks ──────────────────────────────────────────────────────

  private async checkHealth(managed: ManagedContainer): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const req = http.get(
        {
          hostname: '127.0.0.1',
          port: managed.hostPort,
          path: managed.healthPath,
          timeout: 2000,
        },
        (res) => {
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

  /** List all managed replica IDs */
  getManagedReplicas(): string[] {
    return Array.from(this.containers.keys());
  }

  /** Get resource usage (approximate) */
  getResourceUsage(): { cpuUsed: number; memUsed: number } {
    // In a real implementation, we'd query Docker stats
    // For now, return static values based on managed containers
    return {
      cpuUsed: this.containers.size * 100, // rough estimate
      memUsed: this.containers.size * 128,
    };
  }
}
