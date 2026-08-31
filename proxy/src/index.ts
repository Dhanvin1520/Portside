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
// Architecture:
// - Refreshes the backend list every 2 seconds from the CP API
// - Routes each request to the next ready replica (round-robin)
// - Removes unhealthy replicas within one health-check interval
// ─────────────────────────────────────────────────────────────────────────────

const PROXY_PORT = parseInt(process.env.PROXY_PORT || '8080', 10);
const CP_API = process.env.CONTROL_PLANE_API || 'http://control-plane:7000';
const REFRESH_INTERVAL_MS = 2000;

interface Backend {
  replicaId: string;
  node: string;
  hostPort: number;
  deployment: string;
}

// Active backends per service
const serviceBackends: Map<string, Backend[]> = new Map();
const roundRobinIndex: Map<string, number> = new Map();

// ── Backend discovery ────────────────────────────────────────────────────

async function refreshBackends(): Promise<void> {
  try {
    // Fetch services
    const servicesRes = await fetch(`${CP_API}/v1/deployments`);
    if (!servicesRes.ok) return;
    const deployments = (await servicesRes.json()) as any[];

    // Fetch all replicas
    const replicasRes = await fetch(`${CP_API}/v1/replicas`);
    if (!replicasRes.ok) return;
    const replicas = (await replicasRes.json()) as any[];

    // Build backend map: only include ready replicas
    const newBackends = new Map<string, Backend[]>();

    for (const dep of deployments) {
      const depReplicas = replicas.filter(
        (r: any) => r.deploymentName === dep.name && r.ready && r.node && r.hostPort
      );

      newBackends.set(
        dep.name,
        depReplicas.map((r: any) => ({
          replicaId: r.id,
          node: r.node,
          hostPort: r.hostPort,
          deployment: dep.name,
        }))
      );
    }

    // Update active backends
    serviceBackends.clear();
    for (const [name, backends] of newBackends) {
      serviceBackends.set(name, backends);
    }
  } catch (err) {
    log.error('Backend refresh failed', { error: (err as Error).message });
  }
}

// ── Round-robin selection ────────────────────────────────────────────────

function getNextBackend(serviceName: string): Backend | null {
  const backends = serviceBackends.get(serviceName);
  if (!backends || backends.length === 0) return null;

  const idx = (roundRobinIndex.get(serviceName) || 0) % backends.length;
  roundRobinIndex.set(serviceName, idx + 1);
  return backends[idx];
}

// ── Proxy server ─────────────────────────────────────────────────────────

function main(): void {
  const proxy = httpProxy.createProxyServer({});

  proxy.on('error', (err, _req, res) => {
    log.error('Proxy error', { error: err.message });
    if (res && 'writeHead' in res) {
      (res as http.ServerResponse).writeHead(502, { 'Content-Type': 'application/json' });
      (res as http.ServerResponse).end(JSON.stringify({ error: 'Bad Gateway' }));
    }
  });

  const server = http.createServer((req, res) => {
    // Route based on Host header or X-Service header
    // Default: use the first deployment found
    const serviceName = req.headers['x-portside-service'] as string
      || req.headers['host']?.split('.')[0]
      || getFirstService();

    if (!serviceName) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'No services configured' }));
      return;
    }

    const backend = getNextBackend(serviceName);

    if (!backend) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: 'Service Unavailable',
        message: `No ready backends for service "${serviceName}"`,
      }));
      return;
    }

    const target = `http://${backend.node}:${backend.hostPort}`;

    log.debug('Proxying request', {
      service: serviceName,
      backend: backend.replicaId,
      target,
      path: req.url,
    });

    proxy.web(req, res, { target });
  });

  // Start backend refresh loop
  setInterval(refreshBackends, REFRESH_INTERVAL_MS);
  refreshBackends(); // Initial refresh

  server.listen(PROXY_PORT, () => {
    log.info(`Proxy listening on port ${PROXY_PORT}`);
  });

  // Graceful shutdown
  process.on('SIGTERM', () => {
    log.info('Shutting down proxy');
    server.close();
    process.exit(0);
  });
}

function getFirstService(): string | undefined {
  const keys = serviceBackends.keys();
  const first = keys.next();
  return first.done ? undefined : first.value;
}

main();
