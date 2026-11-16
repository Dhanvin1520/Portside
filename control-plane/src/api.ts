import express, { Request, Response, Router } from 'express';
import cors from 'cors';
import {
  createLogger,
  parseSpecs,
  DeploymentSpec,
  ServiceSpec,
  parseCpuString,
  parseMemoryString,
} from '@portside/shared';
import { StateDB } from './db';
import { Reconciler } from './reconciler';
import { Scheduler } from './scheduler';
import { collectDefaultMetrics, register } from 'prom-client';

const log = createLogger('rest-api');

// ─────────────────────────────────────────────────────────────────────────────
// REST API
// Public-facing HTTP API consumed by the CLI and dashboard.
// ─────────────────────────────────────────────────────────────────────────────

export function createRestApi(db: StateDB, reconciler: Reconciler, scheduler: Scheduler): express.Application {
  const app = express();
  app.use(cors());
  app.use(express.json());
  app.use(express.text({ type: 'application/x-yaml' }));

  // Register default metrics once per process (createRestApi may be called
  // more than once, e.g. in tests).
  if (!register.getSingleMetric('process_cpu_user_seconds_total')) {
    collectDefaultMetrics();
  }

  const router = Router();

  // ── Apply (create/update Deployments & Services) ───────────────────────

  router.post('/v1/apply', (req: Request, res: Response) => {
    try {
      const yamlContent = typeof req.body === 'string'
        ? req.body
        : JSON.stringify(req.body);

      const specs = parseSpecs(yamlContent);

      // All documents in one file are applied atomically.
      const results = db.transaction(() => specs.map(spec =>
        spec.kind === 'Deployment'
          ? applyDeployment(db, spec as DeploymentSpec)
          : applyService(db, spec as ServiceSpec)
      ));

      res.json({ applied: results });
    } catch (err: any) {
      log.error('Apply failed', { error: err.message });
      res.status(400).json({ error: err.message });
    }
  });

  // ── Deployments ────────────────────────────────────────────────────────

  router.get('/v1/deployments', (_req: Request, res: Response) => {
    const deployments = db.getAllDeployments();
    const result = deployments.map(dep => {
      const replicas = db.getReplicasForDeployment(dep.name);
      const ready = replicas.filter(r => r.ready).length;
      const active = replicas.filter(r => !['Terminated'].includes(r.state)).length;
      return {
        ...dep,
        readyReplicas: ready,
        activeReplicas: active,
      };
    });
    res.json(result);
  });

  router.get('/v1/deployments/:name', (req: Request, res: Response) => {
    const dep = db.getDeployment(req.params.name);
    if (!dep) return res.status(404).json({ error: 'Deployment not found' });

    const replicas = db.getReplicasForDeployment(dep.name);
    const ready = replicas.filter(r => r.ready).length;
    const active = replicas.filter(r => !['Terminated'].includes(r.state)).length;
    const revision = db.getRevision(dep.name, dep.currentRevision);

    res.json({
      ...dep,
      readyReplicas: ready,
      activeReplicas: active,
      currentSpec: revision ? JSON.parse(revision.specJson) : null,
    });
  });

  // ── Replicas ───────────────────────────────────────────────────────────

  router.get('/v1/replicas', (req: Request, res: Response) => {
    const deployment = req.query.deployment as string | undefined;
    const replicas = deployment
      ? db.getReplicasForDeployment(deployment)
      : db.getAllReplicas();

    // Filter out terminated replicas by default
    const active = replicas.filter(r => r.state !== 'Terminated');
    res.json(active);
  });

  // ── Nodes ──────────────────────────────────────────────────────────────

  router.get('/v1/nodes', (_req: Request, res: Response) => {
    const nodes = db.getAllNodes();
    const result = nodes.map(node => {
      const replicas = db.getReplicasOnNode(node.name)
        .filter(r => !['Terminated'].includes(r.state));
      return {
        ...node,
        replicaCount: replicas.length,
        cpuUtilization: node.cpuCapacity > 0
          ? ((node.cpuAllocated / node.cpuCapacity) * 100).toFixed(1) + '%'
          : '0%',
        memUtilization: node.memCapacity > 0
          ? ((node.memAllocated / node.memCapacity) * 100).toFixed(1) + '%'
          : '0%',
      };
    });
    res.json(result);
  });

  // ── Scale ──────────────────────────────────────────────────────────────

  router.post('/v1/deployments/:name/scale', (req: Request, res: Response) => {
    const dep = db.getDeployment(req.params.name);
    if (!dep) return res.status(404).json({ error: 'Deployment not found' });

    const { replicas } = req.body;
    if (typeof replicas !== 'number' || !Number.isInteger(replicas) || replicas < 0) {
      return res.status(400).json({ error: 'replicas must be a non-negative integer' });
    }

    dep.desiredReplicas = replicas;
    db.upsertDeployment(dep);
    db.addEvent('Scaled', `deployment/${dep.name}`,
      `Scaled to ${replicas} replicas`);

    log.info('Deployment scaled', { deployment: dep.name, replicas });
    res.json({ message: `Deployment ${dep.name} scaled to ${replicas} replicas` });
  });

  // ── Rollback ───────────────────────────────────────────────────────────

  router.post('/v1/deployments/:name/rollback', (req: Request, res: Response) => {
    const dep = db.getDeployment(req.params.name);
    if (!dep) return res.status(404).json({ error: 'Deployment not found' });

    const prevRevision = db.getPreviousRevision(dep.name, dep.currentRevision);
    if (!prevRevision) {
      return res.status(400).json({ error: 'No previous revision to roll back to' });
    }

    // Like Kubernetes, a rollback creates a NEW revision that copies the old
    // spec. Revision numbers only ever grow, so a later apply can never
    // collide with an existing revision.
    const newRevision = db.transaction(() => {
      const next = (db.getLatestRevision(dep.name)?.revision ?? dep.currentRevision) + 1;
      db.insertRevision({
        deploymentName: dep.name,
        revision: next,
        specJson: prevRevision.specJson,
        createdAt: Date.now(),
      });
      dep.currentRevision = next;
      db.upsertDeployment(dep);
      db.addEvent('RolloutRolledBack', `deployment/${dep.name}`,
        `Rolled back to the spec of revision ${prevRevision.revision} (as revision ${next})`);
      return next;
    });

    log.info('Deployment rolled back', {
      deployment: dep.name,
      toSpecOfRevision: prevRevision.revision,
      newRevision,
    });
    res.json({
      message: `Deployment ${dep.name} rolling back to the spec of revision ${prevRevision.revision} (new revision ${newRevision})`,
      revision: newRevision,
    });
  });

  // ── Drain ──────────────────────────────────────────────────────────────

  router.post('/v1/nodes/:name/drain', (req: Request, res: Response) => {
    const node = db.getNode(req.params.name);
    if (!node) return res.status(404).json({ error: 'Node not found' });

    db.updateNodeStatus(node.name, 'Draining');
    db.addEvent('NodeDraining', `node/${node.name}`, 'Node marked for drain');

    // The reconciler evicts replicas from Draining nodes on its next tick,
    // and the scheduler never places new replicas on them.
    log.info('Node drain initiated', { node: node.name });
    res.json({ message: `Node ${node.name} draining` });
  });

  router.post('/v1/nodes/:name/uncordon', (req: Request, res: Response) => {
    const node = db.getNode(req.params.name);
    if (!node) return res.status(404).json({ error: 'Node not found' });
    if (node.status !== 'Draining') {
      return res.status(400).json({ error: `Node ${node.name} is ${node.status}, not Draining` });
    }

    db.updateNodeStatus(node.name, 'Ready');
    db.addEvent('NodeReady', `node/${node.name}`, 'Node uncordoned, schedulable again');
    log.info('Node uncordoned', { node: node.name });
    res.json({ message: `Node ${node.name} uncordoned` });
  });

  // ── Services ───────────────────────────────────────────────────────────

  router.get('/v1/services', (_req: Request, res: Response) => {
    res.json(db.getAllServices());
  });

  // ── Events ─────────────────────────────────────────────────────────────

  router.get('/v1/events', (req: Request, res: Response) => {
    const limit = parseInt(req.query.limit as string) || 100;
    const object = req.query.object as string | undefined;
    const events = object
      ? db.getEventsForObject(object, limit)
      : db.getEvents(limit);
    res.json(events);
  });

  // ── Metrics ────────────────────────────────────────────────────────────

  router.get('/metrics', async (_req: Request, res: Response) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  });

  // ── Health ─────────────────────────────────────────────────────────────

  router.get('/healthz', (_req: Request, res: Response) => {
    res.json({ status: 'ok', uptime: process.uptime() });
  });

  app.use(router);
  return app;
}

// ─────────────────────────────────────────────────────────────────────────────
// Apply helpers
// ─────────────────────────────────────────────────────────────────────────────

function applyDeployment(db: StateDB, spec: DeploymentSpec): any {
  const name = spec.metadata.name;
  const existing = db.getDeployment(name);

  const strategy = spec.spec.strategy?.rollingUpdate || {};
  const maxSurge = strategy.maxSurge ?? 1;
  const maxUnavailable = strategy.maxUnavailable ?? 1;

  if (existing) {
    existing.desiredReplicas = spec.spec.replicas;
    existing.strategy = { maxSurge, maxUnavailable };
    existing.selector = spec.spec.selector.matchLabels;

    // Only a change to the pod template starts a rollout (as in Kubernetes).
    // Changing replicas or strategy just updates the deployment in place.
    const current = db.getRevision(name, existing.currentRevision);
    const currentTemplate = current ? JSON.stringify(JSON.parse(current.specJson).spec?.template) : null;
    if (currentTemplate === JSON.stringify(spec.spec.template)) {
      db.upsertDeployment(existing);
      db.addEvent('Applied', `deployment/${name}`,
        `Deployment configured (${spec.spec.replicas} replicas), template unchanged`);
      log.info('Deployment configured', { name, revision: existing.currentRevision });
      return { kind: 'Deployment', name, action: 'configured', revision: existing.currentRevision };
    }

    // Revision numbers come from the highest stored revision, not the current
    // one: after a rollback the current revision is not the newest.
    const newRevision = (db.getLatestRevision(name)?.revision ?? existing.currentRevision) + 1;
    db.insertRevision({
      deploymentName: name,
      revision: newRevision,
      specJson: JSON.stringify(spec),
      createdAt: Date.now(),
    });

    existing.currentRevision = newRevision;
    db.upsertDeployment(existing);

    db.addEvent('RolloutStarted', `deployment/${name}`,
      `Rolling update to revision ${newRevision}`);

    log.info('Deployment updated', { name, revision: newRevision });
    return { kind: 'Deployment', name, action: 'updated', revision: newRevision };
  } else {
    // Create new deployment
    db.upsertDeployment({
      name,
      currentRevision: 1,
      desiredReplicas: spec.spec.replicas,
      selector: spec.spec.selector.matchLabels,
      strategy: { maxSurge, maxUnavailable },
    });

    db.insertRevision({
      deploymentName: name,
      revision: 1,
      specJson: JSON.stringify(spec),
      createdAt: Date.now(),
    });

    db.addEvent('Applied', `deployment/${name}`,
      `Deployment created with ${spec.spec.replicas} replicas`);

    log.info('Deployment created', { name, replicas: spec.spec.replicas });
    return { kind: 'Deployment', name, action: 'created', revision: 1 };
  }
}

function applyService(db: StateDB, spec: ServiceSpec): any {
  const name = spec.metadata.name;
  const port = spec.spec.ports[0];

  db.upsertService({
    name,
    selector: spec.spec.selector,
    port: port.port,
    targetPort: port.targetPort,
  });

  db.addEvent('Applied', `service/${name}`,
    `Service created/updated, port ${port.port} → ${port.targetPort}`);

  log.info('Service applied', { name });
  return { kind: 'Service', name, action: 'applied' };
}
