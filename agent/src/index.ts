import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import { createLogger } from '@portside/shared';
import { ContainerManager } from './container-manager';

const log = createLogger('agent');

// ─────────────────────────────────────────────────────────────────────────────
// Worker Agent
//
// The agent is intentionally "dumb": it only manages local containers.
// All scheduling and orchestration decisions are made by the control plane.
//
// On startup:
// 1. Register with the control plane (node name + resources)
// 2. Open a bidirectional gRPC stream
// 3. Send heartbeats every 1s with container status
// 4. Execute start/stop commands received from the stream
// ─────────────────────────────────────────────────────────────────────────────

const PROTO_PATH = path.resolve(__dirname, '../../proto/agent.proto');
const HEARTBEAT_INTERVAL_MS = 1000;

const NODE_NAME = process.env.NODE_NAME || 'node-1';
const CONTROL_PLANE = process.env.CONTROL_PLANE || 'control-plane:7001';
const CPU_CAPACITY = parseInt(process.env.CPU || '2000', 10);
const MEM_CAPACITY = parseInt(process.env.MEMORY || '2048', 10);

async function main(): Promise<void> {
  log.info('Starting Portside agent', {
    node: NODE_NAME,
    controlPlane: CONTROL_PLANE,
    cpu: CPU_CAPACITY,
    mem: MEM_CAPACITY,
  });

  const containerManager = new ContainerManager();

  // Load proto
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(packageDef) as any;
  const client = new proto.portside.ControlPlane(
    CONTROL_PLANE,
    grpc.credentials.createInsecure()
  );

  // Step 1: Register
  await new Promise<void>((resolve, reject) => {
    client.Register(
      { nodeName: NODE_NAME, cpuCapacity: CPU_CAPACITY, memCapacity: MEM_CAPACITY },
      (err: Error | null, response: any) => {
        if (err) {
          log.error('Registration failed', { error: err.message });
          reject(err);
          return;
        }
        log.info('Registered with control plane', { response });
        resolve();
      }
    );
  });

  // Step 2: Open bidirectional heartbeat stream
  const stream = client.Heartbeat();

  // Handle commands from control plane
  stream.on('data', async (command: any) => {
    if (command.start) {
      const { start } = command;
      log.info('Received start command', { replicaId: start.replicaId });
      try {
        await containerManager.startContainer({
          replicaId: start.replicaId,
          image: start.image,
          cpuRequest: start.cpuRequest || 100,
          memRequest: start.memRequest || 128,
          ports: (start.ports || []).map((p: any) => ({
            containerPort: p.containerPort,
            hostPort: p.hostPort,
          })),
          env: (start.env || []).map((e: any) => ({
            key: e.key,
            value: e.value,
          })),
          healthPath: start.healthPath || '/healthz',
          healthPort: start.healthPort || 8080,
          healthIntervalMs: start.healthIntervalMs || 5000,
        });
      } catch (err) {
        log.error('Failed to start container', {
          replicaId: start.replicaId,
          error: (err as Error).message,
        });
      }
    }

    if (command.stop) {
      const { stop } = command;
      log.info('Received stop command', { replicaId: stop.replicaId });
      try {
        await containerManager.stopContainer(stop.replicaId, stop.graceSeconds || 30);
      } catch (err) {
        log.error('Failed to stop container', {
          replicaId: stop.replicaId,
          error: (err as Error).message,
        });
      }
    }
  });

  stream.on('error', (err: Error) => {
    log.error('Stream error', { error: err.message });
    // Reconnect logic would go here in production
    process.exit(1);
  });

  stream.on('end', () => {
    log.warn('Stream ended by server');
    process.exit(1);
  });

  // Step 3: Send heartbeats every 1s
  setInterval(async () => {
    try {
      const containers = await containerManager.getContainerStatuses();
      const used = containerManager.getResourceUsage();

      stream.write({
        nodeName: NODE_NAME,
        containers: containers.map(c => ({
          replicaId: c.replicaId,
          state: c.state,
          exitCode: c.exitCode,
          healthCheck: c.healthCheck,
          restartCount: c.restartCount,
        })),
        used: {
          cpuUsed: used.cpuUsed,
          memUsed: used.memUsed,
        },
      });
    } catch (err) {
      log.error('Heartbeat send error', { error: (err as Error).message });
    }
  }, HEARTBEAT_INTERVAL_MS);

  log.info('Agent running, sending heartbeats');

  // Graceful shutdown
  const shutdown = async () => {
    log.info('Shutting down agent...');
    stream.end();
    // Stop all managed containers
    for (const replicaId of containerManager.getManagedReplicas()) {
      await containerManager.stopContainer(replicaId, 5);
    }
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch(err => {
  log.error('Fatal error', { error: err.message });
  process.exit(1);
});
