import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import { createLogger, GrpcContainerStatus } from '@portside/shared';
import { Reconciler } from './reconciler';
import { StateDB } from './db';

const log = createLogger('grpc-server');

// ─────────────────────────────────────────────────────────────────────────────
// gRPC Server
//
// Implements the ControlPlane gRPC service defined in agent.proto.
// Each agent connects via a bidirectional stream (Heartbeat RPC).
// The control plane sends commands DOWN the stream, and agents send
// heartbeats UP.
// ─────────────────────────────────────────────────────────────────────────────

const PROTO_PATH = path.resolve(__dirname, '../../proto/agent.proto');

export class GrpcServer {
  private server: grpc.Server;
  private agentStreams: Map<string, grpc.ServerDuplexStream<any, any>> = new Map();
  private commandPollInterval: ReturnType<typeof setInterval> | null = null;

  constructor(
    private db: StateDB,
    private reconciler: Reconciler,
    private port: number = 7001
  ) {
    this.server = new grpc.Server();
  }

  async start(): Promise<void> {
    const packageDef = protoLoader.loadSync(PROTO_PATH, {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    const proto = grpc.loadPackageDefinition(packageDef) as any;

    this.server.addService(proto.portside.ControlPlane.service, {
      Register: this.handleRegister.bind(this),
      Heartbeat: this.handleHeartbeat.bind(this),
      ListContainers: this.handleListContainers.bind(this),
    });

    return new Promise((resolve, reject) => {
      this.server.bindAsync(
        `0.0.0.0:${this.port}`,
        grpc.ServerCredentials.createInsecure(),
        (err) => {
          if (err) {
            log.error('gRPC server failed to bind', { error: err.message });
            reject(err);
            return;
          }
          log.info('gRPC server started', { port: this.port });

          // Poll for commands to send to agents
          this.commandPollInterval = setInterval(() => {
            this.sendPendingCommands();
          }, 200);

          resolve();
        }
      );
    });
  }

  stop(): void {
    if (this.commandPollInterval) {
      clearInterval(this.commandPollInterval);
    }
    this.server.forceShutdown();
    log.info('gRPC server stopped');
  }

  // ── RPC Handlers ─────────────────────────────────────────────────────────

  private handleRegister(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): void {
    const { nodeName, cpuCapacity, memCapacity } = call.request;
    log.info('Agent registering', { node: nodeName, cpu: cpuCapacity, mem: memCapacity });

    this.db.upsertNode({
      name: nodeName,
      cpuCapacity: cpuCapacity || 2000,
      memCapacity: memCapacity || 2048,
      cpuAllocated: 0,
      memAllocated: 0,
      status: 'Ready',
      lastHeartbeatAt: Date.now(),
    });

    this.db.addEvent('NodeReady', `node/${nodeName}`,
      `Node registered with ${cpuCapacity}m CPU, ${memCapacity}Mi memory`);

    callback(null, { accepted: true, message: `Node ${nodeName} registered` });
  }

  private handleHeartbeat(
    call: grpc.ServerDuplexStream<any, any>
  ): void {
    let nodeName = '';

    call.on('data', (request: any) => {
      nodeName = request.nodeName || nodeName;

      if (nodeName && !this.agentStreams.has(nodeName)) {
        this.agentStreams.set(nodeName, call);
        log.info('Agent stream connected', { node: nodeName });
      }

      // Process heartbeat
      const containers: GrpcContainerStatus[] = (request.containers || []).map((c: any) => ({
        replicaId: c.replicaId,
        state: c.state,
        exitCode: c.exitCode || 0,
        healthCheck: c.healthCheck || false,
        restartCount: c.restartCount || 0,
      }));

      const used = request.used || { cpuUsed: 0, memUsed: 0 };

      this.reconciler.processHeartbeat(nodeName, containers, used);
    });

    call.on('end', () => {
      if (nodeName) {
        log.warn('Agent stream disconnected', { node: nodeName });
        this.agentStreams.delete(nodeName);
      }
      call.end();
    });

    call.on('error', (err: Error) => {
      if (nodeName) {
        log.warn('Agent stream error', { node: nodeName, error: err.message });
        this.agentStreams.delete(nodeName);
      }
    });
  }

  private handleListContainers(
    call: grpc.ServerUnaryCall<any, any>,
    callback: grpc.sendUnaryData<any>
  ): void {
    const { nodeName } = call.request;
    log.info('ListContainers requested', { node: nodeName });

    // We respond with what we expect on this node, so the agent can reconcile
    const replicas = this.db.getReplicasOnNode(nodeName);
    const containers = replicas
      .filter(r => !['Terminated', 'Pending'].includes(r.state))
      .map(r => ({
        replicaId: r.id,
        state: r.state.toLowerCase(),
        exitCode: 0,
        healthCheck: r.ready,
        restartCount: r.restartCount,
      }));

    callback(null, { containers });
  }

  // ── Command delivery ─────────────────────────────────────────────────────

  private sendPendingCommands(): void {
    for (const [nodeName, stream] of this.agentStreams) {
      const commands = this.reconciler.drainCommands(nodeName);
      for (const cmd of commands) {
        try {
          if (cmd.kind === 'start') {
            stream.write({ start: cmd.start });
          } else if (cmd.kind === 'stop') {
            stream.write({ stop: cmd.stop });
          }
        } catch (err) {
          log.error('Failed to send command to agent', {
            node: nodeName,
            error: (err as Error).message,
          });
        }
      }
    }
  }
}
