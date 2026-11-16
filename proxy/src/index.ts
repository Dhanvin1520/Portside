import http from 'http';
import httpProxy from 'http-proxy';
import { createLogger } from '@portside/shared';

const log = createLogger('proxy');

// ─────────────────────────────────────────────────────────────────────────────
// Service Proxy
//
// Round-robin load balancer that routes traffic only to ready replicas.
// Polls the control plane REST API to discover healthy backends.
//
// - Services are resolved by label selector: a Service selects every
//   Deployment whose matchLabels contain all of the Service's selector labels.
//   Deployment names also work as routing keys.
// - Backends refresh every second. The control plane keeps a terminating
//   replica running for 1.5 s after marking it not-ready, so the proxy drops
//   it before its container is stopped.
// - Idempotent requests (GET/HEAD/OPTIONS) that fail to connect are retried on
//   the next backend, which covers the remaining small race windows.
// ─────────────────────────────────────────────────────────────────────────────

const PROXY_PORT = parseInt(process.env.PROXY_PORT || '8080', 10);
const CP_API = process.env.CONTROL_PLANE_API || 'http://control-plane:7000';
const REFRESH_INTERVAL_MS = parseInt(process.env.REFRESH_INTERVAL_MS || '1000', 10);
const RETRYABLE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

interface Backend {
  replicaId: string;
  node: string;
  hostPort: number;
}

// Routing key (service or deployment name) -> ready backends
let routes: Map<string, Backend[]> = new Map();
let serviceNames: string[] = [];
const roundRobinIndex: Map<string, number> = new Map();

// ── Backend discovery ────────────────────────────────────────────────────

async function getJson(path: string): Promise<any> {
  const res = await fetch(`${CP_API}${path}`);
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return res.json();
}

async function refreshBackends(): Promise<void> {
  try {
    const [services, deployments, replicas] = await Promise.all([
      getJson('/v1/services'),
      getJson('/v1/deployments'),
      getJson('/v1/replicas'),
    ]) as [any[], any[], any[]];

    const readyByDeployment = new Map<string, Backend[]>();
    for (const r of replicas) {
      if (!r.ready || !r.node || !r.hostPort) continue;
      const list = readyByDeployment.get(r.deploymentName) || [];
      list.push({ replicaId: r.id, node: r.node, hostPort: r.hostPort });
      readyByDeployment.set(r.deploymentName, list);
    }

    const next = new Map<string, Backend[]>();
    for (const dep of deployments) {
      next.set(dep.name, readyByDeployment.get(dep.name) || []);
    }
    for (const svc of services) {
      const selected = deployments.filter(dep =>
        Object.entries(svc.selector || {}).every(([k, v]) => dep.selector?.[k] === v)
      );
      next.set(svc.name, selected.flatMap(dep => readyByDeployment.get(dep.name) || []));
    }

    routes = next;
    serviceNames = services.map(s => s.name);
  } catch (err) {
    // Keep serving with the last known backends.
    log.error('Backend refresh failed', { error: (err as Error).message });
  }
}

// ── Routing ──────────────────────────────────────────────────────────────

/** Pick the routing key: header, then Host's first label, then the default. */
function resolveRoute(req: http.IncomingMessage): string | undefined {
  const header = req.headers['x-portside-service'];
  if (typeof header === 'string' && routes.has(header)) return header;

  const hostLabel = req.headers.host?.split(':')[0].split('.')[0];
  if (hostLabel && routes.has(hostLabel)) return hostLabel;

  // Default: the first Service, else the first deployment.
  return serviceNames[0] ?? routes.keys().next().value;
}

function nextBackend(route: string): Backend | null {
  const backends = routes.get(route);
  if (!backends || backends.length === 0) return null;
  const idx = (roundRobinIndex.get(route) || 0) % backends.length;
  roundRobinIndex.set(route, idx + 1);
  return backends[idx];
}

// ── Proxy server ─────────────────────────────────────────────────────────

function main(): void {
  const proxy = httpProxy.createProxyServer({ proxyTimeout: 10000 });

  const forward = (
    req: http.IncomingMessage,
    res: http.ServerResponse,
    route: string,
    attempt: number
  ): void => {
    const backend = nextBackend(route);
    if (!backend) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Service Unavailable', message: `No ready backends for "${route}"` }));
      return;
    }

    const target = `http://${backend.node}:${backend.hostPort}`;
    proxy.web(req, res, { target }, (err: Error) => {
      const maxAttempts = Math.max(1, routes.get(route)?.length || 1);
      const canRetry = RETRYABLE_METHODS.has(req.method || 'GET') && !res.headersSent;
      if (canRetry && attempt + 1 < maxAttempts) {
        log.warn('Backend failed, retrying on next backend', {
          route, backend: backend.replicaId, error: err.message, attempt: attempt + 1,
        });
        forward(req, res, route, attempt + 1);
        return;
      }
      log.error('Proxy error', { route, backend: backend.replicaId, error: err.message });
      if (!res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'application/json' });
      }
      res.end(JSON.stringify({ error: 'Bad Gateway' }));
    });
  };

  const server = http.createServer((req, res) => {
    const route = resolveRoute(req);
    if (!route) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'No services configured' }));
      return;
    }
    forward(req, res, route, 0);
  });

  setInterval(refreshBackends, REFRESH_INTERVAL_MS);
  void refreshBackends();

  server.listen(PROXY_PORT, () => {
    log.info(`Proxy listening on port ${PROXY_PORT}`);
  });

  process.on('SIGTERM', () => {
    log.info('Shutting down proxy');
    server.close();
    process.exit(0);
  });
}

main();
