// ─────────────────────────────────────────────────────────────────────────────
// Portside Shared Types
// Core data model for the entire orchestrator
// ─────────────────────────────────────────────────────────────────────────────

// ── Node ─────────────────────────────────────────────────────────────────────

export type NodeStatus = 'Ready' | 'NotReady' | 'Draining';

export interface ClusterNode {
  name: string;
  cpuCapacity: number;   // millicores
  memCapacity: number;   // MiB
  cpuAllocated: number;
  memAllocated: number;
  status: NodeStatus;
  lastHeartbeatAt: number; // epoch ms
}

// ── Deployment ───────────────────────────────────────────────────────────────

export interface DeploymentSpec {
  apiVersion: string;
  kind: 'Deployment';
  metadata: {
    name: string;
    labels?: Record<string, string>;
  };
  spec: {
    replicas: number;
    selector: {
      matchLabels: Record<string, string>;
    };
    strategy?: {
      type: 'RollingUpdate';
      rollingUpdate?: {
        maxSurge?: number;
        maxUnavailable?: number;
      };
    };
    template: {
      metadata?: {
        labels?: Record<string, string>;
      };
      spec: {
        containers: ContainerSpec[];
      };
    };
  };
}

export interface ContainerSpec {
  name: string;
  image: string;
  ports?: { containerPort: number }[];
  resources?: {
    requests?: { cpu?: string; memory?: string };
    limits?: { cpu?: string; memory?: string };
  };
  env?: { name: string; value: string }[];
  readinessProbe?: HealthProbe;
  livenessProbe?: HealthProbe;
}

export interface HealthProbe {
  httpGet?: { path: string; port: number };
  initialDelaySeconds?: number;
  periodSeconds?: number;
  failureThreshold?: number;
}

// ── Service ──────────────────────────────────────────────────────────────────

export interface ServiceSpec {
  apiVersion: string;
  kind: 'Service';
  metadata: {
    name: string;
  };
  spec: {
    selector: Record<string, string>;
    ports: { port: number; targetPort: number; protocol?: string }[];
    type?: 'ClusterIP' | 'NodePort';
  };
}

// ── Replica ──────────────────────────────────────────────────────────────────

export type ReplicaState =
  | 'Pending'
  | 'Scheduling'
  | 'Starting'
  | 'Running'
  | 'Ready'
  | 'Unhealthy'
  | 'Terminating'
  | 'Terminated'
  | 'CrashLoopBackOff';

export interface Replica {
  id: string;
  deploymentName: string;
  revision: number;
  node: string | null;
  state: ReplicaState;
  restartCount: number;
  ready: boolean;
  containerPort: number;
  hostPort: number;
  lastStateChange: number;  // epoch ms
  nextRestartAt?: number;   // epoch ms, for backoff
  image: string;
  cpuRequest: number;       // millicores
  memRequest: number;       // MiB
}

// ── Revision ─────────────────────────────────────────────────────────────────

export interface Revision {
  deploymentName: string;
  revision: number;
  specJson: string;
  createdAt: number;
}

// ── Deployment (stored state) ────────────────────────────────────────────────

export interface Deployment {
  name: string;
  currentRevision: number;
  desiredReplicas: number;
  selector: Record<string, string>;
  strategy: {
    maxSurge: number;
    maxUnavailable: number;
  };
}

// ── Service (stored state) ───────────────────────────────────────────────────

export interface Service {
  name: string;
  selector: Record<string, string>;
  port: number;
  targetPort: number;
}

// ── Event ────────────────────────────────────────────────────────────────────

export type EventKind =
  | 'Scheduled'
  | 'Started'
  | 'Ready'
  | 'Unhealthy'
  | 'Crashed'
  | 'Restarted'
  | 'Rescheduled'
  | 'NodeReady'
  | 'NodeNotReady'
  | 'NodeDraining'
  | 'RolloutStarted'
  | 'RolloutCompleted'
  | 'RolloutRolledBack'
  | 'Scaled'
  | 'Applied'
  | 'Terminated';

export interface ClusterEvent {
  id: number;
  time: number;
  kind: EventKind;
  object: string;   // e.g. "replica/web-abc123" or "node/node-1"
  message: string;
}

// ── YAML spec (combined) ─────────────────────────────────────────────────────

export type PortsideSpec = DeploymentSpec | ServiceSpec;

// ── gRPC types (mirrors the proto) ───────────────────────────────────────────

export interface GrpcContainerStatus {
  replicaId: string;
  state: string;
  exitCode: number;
  healthCheck: boolean;
  restartCount: number;
}

export interface GrpcHeartbeat {
  nodeName: string;
  containers: GrpcContainerStatus[];
  used: { cpuUsed: number; memUsed: number };
}

export interface GrpcStartContainer {
  replicaId: string;
  image: string;
  cpuRequest: number;
  memRequest: number;
  ports: { containerPort: number; hostPort: number }[];
  env: { key: string; value: string }[];
  healthPath: string;
  healthPort: number;
  healthIntervalMs: number;
}

export interface GrpcStopContainer {
  replicaId: string;
  graceSeconds: number;
}

export type GrpcCommand =
  | { kind: 'start'; start: GrpcStartContainer }
  | { kind: 'stop'; stop: GrpcStopContainer };
