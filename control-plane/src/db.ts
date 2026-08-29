import type { DatabaseSync as DatabaseSyncType } from 'node:sqlite';
// eslint-disable-next-line @typescript-eslint/no-var-requires
const sqliteModule: any = typeof require !== 'undefined' ? require('node:sqlite') : {};
const DatabaseSync: any = sqliteModule.DatabaseSync || (sqliteModule.default && sqliteModule.default.DatabaseSync);
import path from 'path';
import fs from 'fs';
import {
  ClusterNode,
  Deployment,
  Replica,
  Revision,
  Service,
  ClusterEvent,
  NodeStatus,
  ReplicaState,
  EventKind,
} from '@portside/shared';
import { createLogger } from '@portside/shared';

const log = createLogger('db');

// ─────────────────────────────────────────────────────────────────────────────
// SQLite Database Layer (node:sqlite)
// Persistent state store for the control plane. All cluster state is stored
// here and recovered on restart without external native C++ dependencies.
// ─────────────────────────────────────────────────────────────────────────────

export class StateDB {
  private db: DatabaseSyncType;

  constructor(dbPath?: string) {
    const resolvedPath = dbPath || path.join(process.env.DATA_DIR || '/data', 'portside.db');
    if (resolvedPath !== ':memory:') {
      const dir = path.dirname(resolvedPath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
    }
    this.db = new DatabaseSync(resolvedPath);
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec('PRAGMA foreign_keys = ON;');
    this.initialize();
    log.info('Database initialized', { path: resolvedPath });
  }

  private initialize(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS nodes (
        name TEXT PRIMARY KEY,
        cpu_capacity INTEGER NOT NULL,
        mem_capacity INTEGER NOT NULL,
        cpu_allocated INTEGER NOT NULL DEFAULT 0,
        mem_allocated INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'NotReady',
        last_heartbeat_at INTEGER NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS deployments (
        name TEXT PRIMARY KEY,
        current_revision INTEGER NOT NULL DEFAULT 1,
        desired_replicas INTEGER NOT NULL,
        selector_json TEXT NOT NULL,
        max_surge INTEGER NOT NULL DEFAULT 1,
        max_unavailable INTEGER NOT NULL DEFAULT 1
      );

      CREATE TABLE IF NOT EXISTS revisions (
        deployment TEXT NOT NULL,
        revision INTEGER NOT NULL,
        spec_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (deployment, revision),
        FOREIGN KEY (deployment) REFERENCES deployments(name) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS replicas (
        id TEXT PRIMARY KEY,
        deployment TEXT NOT NULL,
        revision INTEGER NOT NULL,
        node TEXT,
        state TEXT NOT NULL DEFAULT 'Pending',
        restart_count INTEGER NOT NULL DEFAULT 0,
        ready INTEGER NOT NULL DEFAULT 0,
        container_port INTEGER NOT NULL DEFAULT 0,
        host_port INTEGER NOT NULL DEFAULT 0,
        last_state_change INTEGER NOT NULL,
        next_restart_at INTEGER,
        image TEXT NOT NULL,
        cpu_request INTEGER NOT NULL DEFAULT 100,
        mem_request INTEGER NOT NULL DEFAULT 128,
        FOREIGN KEY (deployment) REFERENCES deployments(name) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS services (
        name TEXT PRIMARY KEY,
        selector_json TEXT NOT NULL,
        port INTEGER NOT NULL,
        target_port INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time INTEGER NOT NULL,
        kind TEXT NOT NULL,
        object TEXT NOT NULL,
        message TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_replicas_deployment ON replicas(deployment);
      CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node);
      CREATE INDEX IF NOT EXISTS idx_replicas_state ON replicas(state);
      CREATE INDEX IF NOT EXISTS idx_events_time ON events(time);
      CREATE INDEX IF NOT EXISTS idx_events_object ON events(object);
    `);
  }

  // ── Nodes ────────────────────────────────────────────────────────────────

  upsertNode(node: ClusterNode): void {
    this.db.prepare(`
      INSERT INTO nodes (name, cpu_capacity, mem_capacity, cpu_allocated, mem_allocated, status, last_heartbeat_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(name) DO UPDATE SET
        cpu_capacity = excluded.cpu_capacity,
        mem_capacity = excluded.mem_capacity,
        cpu_allocated = excluded.cpu_allocated,
        mem_allocated = excluded.mem_allocated,
        status = excluded.status,
        last_heartbeat_at = excluded.last_heartbeat_at
    `).run(
      node.name, node.cpuCapacity, node.memCapacity,
      node.cpuAllocated, node.memAllocated,
      node.status, node.lastHeartbeatAt
    );
  }

  getNode(name: string): ClusterNode | undefined {
    const row = this.db.prepare('SELECT * FROM nodes WHERE name = ?').get(name) as any;
    return row ? this.rowToNode(row) : undefined;
  }

  getAllNodes(): ClusterNode[] {
    return (this.db.prepare('SELECT * FROM nodes').all() as any[]).map(this.rowToNode);
  }

  updateNodeStatus(name: string, status: NodeStatus): void {
    this.db.prepare('UPDATE nodes SET status = ? WHERE name = ?').run(status, name);
  }

  updateNodeHeartbeat(name: string, ts: number): void {
    this.db.prepare('UPDATE nodes SET last_heartbeat_at = ?, status = ? WHERE name = ?')
      .run(ts, 'Ready', name);
  }

  updateNodeAllocations(name: string, cpuAllocated: number, memAllocated: number): void {
    this.db.prepare('UPDATE nodes SET cpu_allocated = ?, mem_allocated = ? WHERE name = ?')
      .run(cpuAllocated, memAllocated, name);
  }

  private rowToNode(row: any): ClusterNode {
    return {
      name: row.name,
      cpuCapacity: row.cpu_capacity,
      memCapacity: row.mem_capacity,
      cpuAllocated: row.cpu_allocated,
      memAllocated: row.mem_allocated,
      status: row.status as NodeStatus,
      lastHeartbeatAt: row.last_heartbeat_at,
    };
  }

  // ── Deployments ──────────────────────────────────────────────────────────

  upsertDeployment(dep: Deployment): void {
    this.db.prepare(`
      INSERT INTO deployments (name, current_revision, desired_replicas, selector_json, max_surge, max_unavailable)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(name) DO UPDATE SET
        current_revision = excluded.current_revision,
        desired_replicas = excluded.desired_replicas,
        selector_json = excluded.selector_json,
        max_surge = excluded.max_surge,
        max_unavailable = excluded.max_unavailable
    `).run(
      dep.name, dep.currentRevision, dep.desiredReplicas,
      JSON.stringify(dep.selector), dep.strategy.maxSurge, dep.strategy.maxUnavailable
    );
  }

  getDeployment(name: string): Deployment | undefined {
    const row = this.db.prepare('SELECT * FROM deployments WHERE name = ?').get(name) as any;
    return row ? this.rowToDeployment(row) : undefined;
  }

  getAllDeployments(): Deployment[] {
    return (this.db.prepare('SELECT * FROM deployments').all() as any[]).map(this.rowToDeployment);
  }

  deleteDeployment(name: string): void {
    this.db.prepare('DELETE FROM deployments WHERE name = ?').run(name);
  }

  private rowToDeployment(row: any): Deployment {
    return {
      name: row.name,
      currentRevision: row.current_revision,
      desiredReplicas: row.desired_replicas,
      selector: JSON.parse(row.selector_json),
      strategy: { maxSurge: row.max_surge, maxUnavailable: row.max_unavailable },
    };
  }

  // ── Revisions ────────────────────────────────────────────────────────────

  insertRevision(rev: Revision): void {
    this.db.prepare(`
      INSERT INTO revisions (deployment, revision, spec_json, created_at)
      VALUES (?, ?, ?, ?)
    `).run(rev.deploymentName, rev.revision, rev.specJson, rev.createdAt);
  }

  getRevision(deployment: string, revision: number): Revision | undefined {
    const row = this.db.prepare(
      'SELECT * FROM revisions WHERE deployment = ? AND revision = ?'
    ).get(deployment, revision) as any;
    return row ? this.rowToRevision(row) : undefined;
  }

  getLatestRevision(deployment: string): Revision | undefined {
    const row = this.db.prepare(
      'SELECT * FROM revisions WHERE deployment = ? ORDER BY revision DESC LIMIT 1'
    ).get(deployment) as any;
    return row ? this.rowToRevision(row) : undefined;
  }

  getPreviousRevision(deployment: string, currentRevision: number): Revision | undefined {
    const row = this.db.prepare(
      'SELECT * FROM revisions WHERE deployment = ? AND revision < ? ORDER BY revision DESC LIMIT 1'
    ).get(deployment, currentRevision) as any;
    return row ? this.rowToRevision(row) : undefined;
  }

  private rowToRevision(row: any): Revision {
    return {
      deploymentName: row.deployment,
      revision: row.revision,
      specJson: row.spec_json,
      createdAt: row.created_at,
    };
  }

  // ── Replicas ─────────────────────────────────────────────────────────────

  insertReplica(replica: Replica): void {
    this.db.prepare(`
      INSERT INTO replicas (id, deployment, revision, node, state, restart_count, ready,
        container_port, host_port, last_state_change, next_restart_at, image, cpu_request, mem_request)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      replica.id, replica.deploymentName, replica.revision,
      replica.node, replica.state, replica.restartCount, replica.ready ? 1 : 0,
      replica.containerPort, replica.hostPort,
      replica.lastStateChange, replica.nextRestartAt || null,
      replica.image, replica.cpuRequest, replica.memRequest
    );
  }

  updateReplica(replica: Replica): void {
    this.db.prepare(`
      UPDATE replicas SET
        node = ?, state = ?, restart_count = ?, ready = ?,
        container_port = ?, host_port = ?,
        last_state_change = ?, next_restart_at = ?,
        image = ?, cpu_request = ?, mem_request = ?, revision = ?
      WHERE id = ?
    `).run(
      replica.node, replica.state, replica.restartCount, replica.ready ? 1 : 0,
      replica.containerPort, replica.hostPort,
      replica.lastStateChange, replica.nextRestartAt || null,
      replica.image, replica.cpuRequest, replica.memRequest,
      replica.revision, replica.id
    );
  }

  getReplica(id: string): Replica | undefined {
    const row = this.db.prepare('SELECT * FROM replicas WHERE id = ?').get(id) as any;
    return row ? this.rowToReplica(row) : undefined;
  }

  getReplicasForDeployment(deployment: string): Replica[] {
    return (this.db.prepare('SELECT * FROM replicas WHERE deployment = ?')
      .all(deployment) as any[]).map(this.rowToReplica);
  }

  getReplicasOnNode(node: string): Replica[] {
    return (this.db.prepare('SELECT * FROM replicas WHERE node = ?')
      .all(node) as any[]).map(this.rowToReplica);
  }

  getAllReplicas(): Replica[] {
    return (this.db.prepare('SELECT * FROM replicas').all() as any[]).map(this.rowToReplica);
  }

  deleteReplica(id: string): void {
    this.db.prepare('DELETE FROM replicas WHERE id = ?').run(id);
  }

  private rowToReplica(row: any): Replica {
    return {
      id: row.id,
      deploymentName: row.deployment,
      revision: row.revision,
      node: row.node,
      state: row.state as ReplicaState,
      restartCount: row.restart_count,
      ready: row.ready === 1,
      containerPort: row.container_port,
      hostPort: row.host_port,
      lastStateChange: row.last_state_change,
      nextRestartAt: row.next_restart_at || undefined,
      image: row.image,
      cpuRequest: row.cpu_request,
      memRequest: row.mem_request,
    };
  }

  // ── Services ─────────────────────────────────────────────────────────────

  upsertService(svc: Service): void {
    this.db.prepare(`
      INSERT INTO services (name, selector_json, port, target_port)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(name) DO UPDATE SET
        selector_json = excluded.selector_json,
        port = excluded.port,
        target_port = excluded.target_port
    `).run(svc.name, JSON.stringify(svc.selector), svc.port, svc.targetPort);
  }

  getService(name: string): Service | undefined {
    const row = this.db.prepare('SELECT * FROM services WHERE name = ?').get(name) as any;
    return row ? this.rowToService(row) : undefined;
  }

  getAllServices(): Service[] {
    return (this.db.prepare('SELECT * FROM services').all() as any[]).map(this.rowToService);
  }

  private rowToService(row: any): Service {
    return {
      name: row.name,
      selector: JSON.parse(row.selector_json),
      port: row.port,
      targetPort: row.target_port,
    };
  }

  // ── Events ───────────────────────────────────────────────────────────────

  addEvent(kind: EventKind, object: string, message: string): void {
    this.db.prepare(
      'INSERT INTO events (time, kind, object, message) VALUES (?, ?, ?, ?)'
    ).run(Date.now(), kind, object, message);
  }

  getEvents(limit = 100): ClusterEvent[] {
    return (this.db.prepare(
      'SELECT * FROM events ORDER BY time DESC LIMIT ?'
    ).all(limit) as any[]).map(row => ({
      id: row.id,
      time: row.time,
      kind: row.kind as EventKind,
      object: row.object,
      message: row.message,
    }));
  }

  getEventsForObject(object: string, limit = 50): ClusterEvent[] {
    return (this.db.prepare(
      'SELECT * FROM events WHERE object = ? ORDER BY time DESC LIMIT ?'
    ).all(object, limit) as any[]).map(row => ({
      id: row.id,
      time: row.time,
      kind: row.kind as EventKind,
      object: row.object,
      message: row.message,
    }));
  }

  // ── Transactions ─────────────────────────────────────────────────────────

  transaction<T>(fn: () => T): T {
    this.db.exec('BEGIN');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  close(): void {
    this.db.close();
  }
}
