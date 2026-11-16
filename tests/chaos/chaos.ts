import { execFileSync } from 'child_process';
import fs from 'fs';
import path from 'path';

// ─────────────────────────────────────────────────────────────────────────────
// Chaos Engineering Harness (runs against the real docker-compose cluster)
//
//   docker compose up -d --build      # from the repo root
//   npm run chaos
//
// Scenarios, each repeated and reported as median / worst case:
//   1. Container crash   kill an app container inside a node, time until the
//                        same replica is Ready again
//   2. Node failure      hard-stop a worker node, time until it is NotReady,
//                        its replicas are rescheduled, and the deployment is
//                        fully Ready again; then restart it and check that
//                        no duplicate containers survive
//   3. Network partition disconnect a node from the network, then reconnect it
//                        and check fencing removes the stale copies
//   4. Rolling update    steady HTTP load through the proxy during a rollout,
//                        a rollback and a re-apply; every non-2xx response or
//                        connection error counts as a failed request
//   5. Control plane     SIGKILL the control plane and start it again, check
//      restart           how long recovery takes and that no app container
//                        was restarted
//   6. Scheduling        per-replica scheduling latency parsed from the
//      latency           control plane's own logs
//
// Nothing is simulated or hardcoded. If the cluster is unreachable the
// harness exits with an error. A target that is missed is reported as FAIL
// and the process exits non-zero. Results are written to
// tests/chaos/results/latest.{json,md}.
// ─────────────────────────────────────────────────────────────────────────────

const ROOT = path.resolve(__dirname, '../..');
const COMPOSE_FILE = process.env.PORTSIDE_COMPOSE_FILE || path.join(ROOT, 'docker-compose.yml');
const API = process.env.CONTROL_PLANE_API || 'http://localhost:7000';
const PROXY = process.env.PROXY_URL || 'http://localhost:8080';
const ITERATIONS = parseInt(process.env.CHAOS_ITERATIONS || '5', 10);
const NODES = ['node-1', 'node-2', 'node-3'];
const DEPLOYMENT = 'web-app';
const V1 = path.join(ROOT, 'examples/web-app/deployment.yaml');
const V2 = path.join(ROOT, 'examples/web-app/deployment-v2.yaml');
const RESULTS_DIR = path.join(__dirname, 'results');

// ── Small helpers ──────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const log = (msg: string) => console.log(msg);

function sh(cmd: string, args: string[]): string {
  return execFileSync(cmd, args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

function compose(...args: string[]): string {
  return sh('docker', ['compose', '-f', COMPOSE_FILE, ...args]);
}

const containerIds = new Map<string, string>();
function containerOf(service: string): string {
  if (!containerIds.has(service)) {
    const id = compose('ps', '-a', '-q', service).split('\n')[0];
    if (!id) throw new Error(`No container for compose service "${service}". Is the cluster up?`);
    containerIds.set(service, id);
  }
  return containerIds.get(service)!;
}

/** Run a docker command inside a worker node (each node runs its own dockerd). */
function nodeDocker(node: string, ...args: string[]): string {
  return sh('docker', ['exec', containerOf(node), 'docker', ...args]);
}

/** Running Portside containers on a node: replicaId -> container id. */
function appContainersOn(node: string): Map<string, string> {
  const out = nodeDocker(node, 'ps', '--filter', 'label=portside.managed=true',
    '--format', '{{.ID}} {{.Label "portside.replica-id"}}');
  const map = new Map<string, string>();
  for (const line of out.split('\n').filter(Boolean)) {
    const [id, replicaId] = line.split(' ');
    map.set(replicaId, id);
  }
  return map;
}

async function api<T = any>(p: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API}${p}`, init);
  const body = await res.text();
  if (!res.ok) throw new Error(`${init?.method || 'GET'} ${p}: HTTP ${res.status} ${body}`);
  return JSON.parse(body) as T;
}

async function apiOrNull<T = any>(p: string): Promise<T | null> {
  try { return await api<T>(p); } catch { return null; }
}

async function applyFile(file: string): Promise<any> {
  return api('/v1/apply', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-yaml' },
    body: fs.readFileSync(file, 'utf8'),
  });
}

class Timeout extends Error {}

/** Poll until cond() is true. Returns elapsed ms since `since`. */
async function waitFor(what: string, cond: () => Promise<boolean> | boolean, timeoutMs: number,
                       since = Date.now(), pollMs = 100): Promise<number> {
  while (Date.now() - since < timeoutMs) {
    try {
      if (await cond()) return Date.now() - since;
    } catch { /* API may be down mid-scenario; keep polling */ }
    await sleep(pollMs);
  }
  throw new Timeout(`Timed out after ${timeoutMs} ms waiting for: ${what}`);
}

async function activeReplicas(): Promise<any[]> {
  const rs = await api<any[]>(`/v1/replicas?deployment=${DEPLOYMENT}`);
  return rs.filter(r => r.state !== 'Terminating');
}

async function deploymentFullyReady(revision?: number): Promise<boolean> {
  const dep = await api<any>(`/v1/deployments/${DEPLOYMENT}`);
  const rs = await activeReplicas();
  return rs.length === dep.desiredReplicas &&
    rs.every(r => r.ready && r.revision === (revision ?? dep.currentRevision));
}

async function nodeStatus(node: string): Promise<string | undefined> {
  const nodes = await api<any[]>('/v1/nodes');
  return nodes.find(n => n.name === node)?.status;
}

/** Replica IDs running on more than one node, and containers on a node they are not assigned to. */
async function duplicatesAndStrays(): Promise<{ duplicates: number; strays: number }> {
  const owners = new Map((await activeReplicas()).map(r => [r.id, r.node]));
  const seen = new Map<string, number>();
  let strays = 0;
  for (const node of NODES) {
    for (const replicaId of appContainersOn(node).keys()) {
      seen.set(replicaId, (seen.get(replicaId) || 0) + 1);
      if (owners.get(replicaId) !== node) strays++;
    }
  }
  const duplicates = [...seen.values()].filter(n => n > 1).length;
  return { duplicates, strays };
}

// ── Statistics & reporting ────────────────────────────────────────────────

interface Row {
  scenario: string;
  target: string;
  samples: number;
  median: string;
  worst: string;
  status: 'PASS' | 'FAIL';
  notes?: string;
}
const rows: Row[] = [];
const raw: Record<string, unknown> = {};

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const secs = (ms: number) => `${(ms / 1000).toFixed(2)} s`;

function timingRow(scenario: string, targetMs: number | null, samples: number[], failures: string[]) {
  raw[scenario] = { samplesMs: samples, failures };
  const ok = failures.length === 0 && samples.length > 0 &&
    (targetMs === null || Math.max(...samples) < targetMs);
  rows.push({
    scenario,
    target: targetMs === null ? 'report only' : `< ${secs(targetMs)}`,
    samples: samples.length,
    median: samples.length ? secs(median(samples)) : '-',
    worst: samples.length ? secs(Math.max(...samples)) : '-',
    status: targetMs === null && failures.length === 0 && samples.length > 0 ? 'PASS' : ok ? 'PASS' : 'FAIL',
    notes: failures.length ? failures.join('; ') : undefined,
  });
}

function countRow(scenario: string, target: string, value: number, pass: boolean, notes?: string) {
  raw[scenario] = { value, notes };
  rows.push({ scenario, target, samples: 1, median: String(value), worst: String(value),
    status: pass ? 'PASS' : 'FAIL', notes });
}

// ── Setup ─────────────────────────────────────────────────────────────────

async function setup(): Promise<void> {
  log('Checking cluster...');
  const health = await apiOrNull('/healthz');
  if (!health) {
    throw new Error(`Control plane not reachable at ${API}. Start the cluster first: docker compose up -d --build`);
  }
  NODES.forEach(containerOf); // fail fast if compose services are missing

  await waitFor('3 Ready nodes', async () =>
    (await api<any[]>('/v1/nodes')).filter(n => n.status === 'Ready').length === NODES.length, 60000);

  log('Deploying v1 (first run pulls the image on every node, this can take a while)...');
  await applyFile(V1);
  const ms = await waitFor('v1 fully ready', async () => {
    const dep = await apiOrNull<any>(`/v1/deployments/${DEPLOYMENT}`);
    if (!dep) return false;
    const spec = dep.currentSpec?.spec?.template?.spec?.containers?.[0];
    return (spec?.args || []).some((a: string) => a.includes('v1')) && deploymentFullyReady();
  }, 300000);
  log(`  v1 ready after ${secs(ms)}`);
  await waitFor('proxy serves v1', async () => (await fetchText(PROXY)).includes('v1'), 30000);
}

async function fetchText(url: string): Promise<string> {
  const res = await fetch(url, { signal: AbortSignal.timeout(2000) });
  return res.ok ? res.text() : '';
}

// ── Scenario 1: container crash ───────────────────────────────────────────

async function containerCrash(): Promise<void> {
  log(`\n[1/6] Container crash x${ITERATIONS}`);
  const samples: number[] = [];
  const failures: string[] = [];

  for (let i = 0; i < ITERATIONS; i++) {
    const target = (await activeReplicas()).find(r => r.ready);
    if (!target) { failures.push('no ready replica to kill'); break; }
    const before = target.restartCount;
    const t0 = Date.now();
    nodeDocker(target.node, 'kill', `portside-${target.id}`);
    try {
      const ms = await waitFor(`replica ${target.id} restarted and ready`, async () => {
        const r = (await activeReplicas()).find(x => x.id === target.id);
        return !!r && r.restartCount > before && r.ready;
      }, 60000, t0);
      samples.push(ms);
      log(`  run ${i + 1}: ${target.id} on ${target.node} ready again in ${secs(ms)}`);
    } catch (err) {
      failures.push((err as Error).message);
      log(`  run ${i + 1}: FAILED ${(err as Error).message}`);
    }
    await waitFor('deployment ready', () => deploymentFullyReady(), 60000);
    await sleep(1000);
  }
  timingRow('Crashed container back to ready', 5000, samples, failures);
}

// ── Scenario 2: node failure ──────────────────────────────────────────────

async function nodeFailure(): Promise<void> {
  const runs = Math.max(1, Math.min(3, ITERATIONS));
  log(`\n[2/6] Node failure x${runs}`);
  const detect: number[] = [];
  const moved: number[] = [];
  const recovered: number[] = [];
  const failures: string[] = [];
  let worstDupes = 0;

  for (let i = 0; i < runs; i++) {
    const node = NODES[1 + (i % 2)];
    const victims = (await activeReplicas()).filter(r => r.node === node).map(r => r.id);
    const t0 = Date.now();
    sh('docker', ['stop', '-t', '0', containerOf(node)]); // SIGKILL, restart policy not applied
    try {
      detect.push(await waitFor(`${node} NotReady`, async () => (await nodeStatus(node)) === 'NotReady', 30000, t0));
      moved.push(await waitFor('replicas moved off', async () => {
        const rs = await activeReplicas();
        return victims.every(id => rs.find(r => r.id === id)?.node !== node);
      }, 30000, t0));
      recovered.push(await waitFor('deployment fully ready', () => deploymentFullyReady(), 120000, t0));
      log(`  run ${i + 1}: ${node} NotReady ${secs(detect.at(-1)!)}, moved ${secs(moved.at(-1)!)}, all ready ${secs(recovered.at(-1)!)}`);
    } catch (err) {
      failures.push((err as Error).message);
      log(`  run ${i + 1}: FAILED ${(err as Error).message}`);
    }

    sh('docker', ['start', containerOf(node)]);
    await waitFor(`${node} Ready again`, async () => (await nodeStatus(node)) === 'Ready', 120000);
    try {
      await waitFor('no duplicate or stray containers', async () => {
        const d = await duplicatesAndStrays();
        return d.duplicates === 0 && d.strays === 0;
      }, 30000);
    } catch { /* counted below */ }
    const d = await duplicatesAndStrays();
    worstDupes = Math.max(worstDupes, d.duplicates + d.strays);
    await waitFor('deployment ready', () => deploymentFullyReady(), 60000);
  }

  timingRow('Node failure: detected NotReady', null, detect, failures);
  timingRow('Node failure: replicas rescheduled', 5000, moved, failures);
  timingRow('Node failure: back to full capacity', 10000, recovered, failures);
  countRow('Duplicate/stray containers after node rejoin', '0', worstDupes, worstDupes === 0);
}

// ── Scenario 3: network partition ─────────────────────────────────────────

async function networkPartition(): Promise<void> {
  log('\n[3/6] Network partition');
  const node = 'node-3';
  const id = containerOf(node);
  const networks = Object.keys(JSON.parse(sh('docker', ['inspect', id, '--format', '{{json .NetworkSettings.Networks}}'])));
  const network = networks.find(n => n.includes('portside-net')) || networks[0];
  const failures: string[] = [];
  const samples: number[] = [];

  const t0 = Date.now();
  sh('docker', ['network', 'disconnect', network, id]);
  try {
    samples.push(await waitFor(`${node} NotReady`, async () => (await nodeStatus(node)) === 'NotReady', 30000, t0));
    await waitFor('deployment fully ready elsewhere', () => deploymentFullyReady(), 120000);
  } catch (err) {
    failures.push((err as Error).message);
  } finally {
    sh('docker', ['network', 'connect', '--alias', node, network, id]);
  }

  await waitFor(`${node} Ready again`, async () => (await nodeStatus(node)) === 'Ready', 120000);
  let leftover = 0;
  try {
    await waitFor('stale copies fenced off', async () => {
      const d = await duplicatesAndStrays();
      return d.duplicates === 0 && d.strays === 0;
    }, 30000);
  } catch {
    const d = await duplicatesAndStrays();
    leftover = d.duplicates + d.strays;
  }
  timingRow('Partition: detected NotReady', null, samples, failures);
  countRow('Duplicate/stray containers after partition heals', '0', leftover, leftover === 0 && failures.length === 0);
}

// ── Scenario 4: rolling update under load ─────────────────────────────────

interface LoadResult { total: number; failed: number; errors: Record<string, number>; bodies: Record<string, number> }

function startLoad(workers = 8): { stop: () => Promise<LoadResult> } {
  const result: LoadResult = { total: 0, failed: 0, errors: {}, bodies: {} };
  let running = true;
  const worker = async () => {
    while (running) {
      try {
        const res = await fetch(PROXY, { signal: AbortSignal.timeout(5000) });
        const text = (await res.text()).trim();
        result.total++;
        if (res.status < 200 || res.status >= 300) {
          result.failed++;
          result.errors[`HTTP ${res.status}`] = (result.errors[`HTTP ${res.status}`] || 0) + 1;
        } else {
          result.bodies[text] = (result.bodies[text] || 0) + 1;
        }
      } catch (err) {
        result.total++;
        result.failed++;
        const key = (err as Error).name || 'error';
        result.errors[key] = (result.errors[key] || 0) + 1;
      }
      await sleep(5);
    }
  };
  const all = Array.from({ length: workers }, worker);
  return { stop: async () => { running = false; await Promise.all(all); return result; } };
}

async function rolloutUnderLoad(): Promise<void> {
  log('\n[4/6] Rolling updates under steady HTTP load');
  const steps: { name: string; run: () => Promise<any> }[] = [
    { name: 'apply v2', run: () => applyFile(V2) },
    { name: 'rollout undo', run: () => api(`/v1/deployments/${DEPLOYMENT}/rollback`, { method: 'POST' }) },
    { name: 're-apply v2', run: () => applyFile(V2) },
  ];
  const durations: number[] = [];
  const failures: string[] = [];
  let total = 0;
  let failed = 0;
  const errors: Record<string, number> = {};
  const desired = (await api<any>(`/v1/deployments/${DEPLOYMENT}`)).desiredReplicas;

  for (const step of steps) {
    const load = startLoad();
    await sleep(2000); // steady state before the change
    const t0 = Date.now();
    try {
      await step.run();
      const target = (await api<any>(`/v1/deployments/${DEPLOYMENT}`)).currentRevision;
      durations.push(await waitFor(`${step.name} complete`, async () =>
        (await api<any[]>(`/v1/events?object=deployment/${DEPLOYMENT}&limit=20`))
          .some(e => e.kind === 'RolloutCompleted' && e.message.startsWith(`Revision ${target} `)), 300000, t0));
    } catch (err) {
      failures.push(`${step.name}: ${(err as Error).message}`);
    }
    await sleep(3000); // keep traffic flowing after the rollout
    const r = await load.stop();
    total += r.total;
    failed += r.failed;
    for (const [k, v] of Object.entries(r.errors)) errors[k] = (errors[k] || 0) + v;
    log(`  ${step.name}: ${secs(durations.at(-1) ?? 0)}, ${r.total} requests, ${r.failed} failed, responses ${JSON.stringify(r.bodies)}`);
  }

  timingRow(`Rolling update duration (${desired} replicas)`, null, durations, failures);
  raw['Failed requests during rollouts'] = { total, failed, errors };
  countRow('Failed requests during rollouts', '0 failed', failed, failed === 0 && failures.length === 0 && total > 0,
    `${failed} / ${total} requests over ${steps.length} rollouts` +
    (Object.keys(errors).length ? `; ${JSON.stringify(errors)}` : ''));
}

// ── Scenario 5: control plane restart ─────────────────────────────────────

async function controlPlaneRestart(): Promise<void> {
  log('\n[5/6] Control plane SIGKILL + restart');
  const before = new Map<string, string>();
  for (const node of NODES) for (const [rid, cid] of appContainersOn(node)) before.set(rid, cid);

  const cp = containerOf('control-plane');
  const t0 = Date.now();
  sh('docker', ['kill', cp]);
  await sleep(500);
  sh('docker', ['start', cp]);

  const failures: string[] = [];
  let apiBack = 0;
  let converged = 0;
  try {
    apiBack = await waitFor('API back', async () => !!(await apiOrNull('/healthz')), 60000, t0, 50);
    converged = await waitFor('nodes Ready and deployment ready', async () => {
      const nodes = await api<any[]>('/v1/nodes');
      return nodes.filter(n => n.status === 'Ready').length === NODES.length && await deploymentFullyReady();
    }, 60000, t0);
  } catch (err) {
    failures.push((err as Error).message);
  }
  await sleep(8000); // past the startup grace period: nothing should be rescheduled

  const after = new Map<string, string>();
  for (const node of NODES) for (const [rid, cid] of appContainersOn(node)) after.set(rid, cid);
  const restarted = [...before].filter(([rid, cid]) => after.get(rid) !== cid).length;
  const notReady = (await api<any[]>('/v1/events?limit=200'))
    .filter(e => e.kind === 'NodeNotReady' && e.time > t0).length;

  log(`  API back ${secs(apiBack)}, converged ${secs(converged)}, app containers restarted: ${restarted}, NodeNotReady events: ${notReady}`);
  timingRow('Control plane restart: API back', null, apiBack ? [apiBack] : [], failures);
  timingRow('Control plane restart: cluster converged', 10000, converged ? [converged] : [], failures);
  countRow('App containers restarted by control plane restart', '0', restarted, restarted === 0 && notReady === 0,
    notReady ? `${notReady} NodeNotReady events` : undefined);
}

// ── Scenario 6: scheduling latency from logs ──────────────────────────────

function schedulingLatency(): void {
  log('\n[6/6] Scheduling latency (from control plane logs)');
  const logs = compose('logs', '--no-color', 'control-plane');
  const samples = [...logs.matchAll(/Scheduled replica .*"latencyMs":"([\d.]+)"/g)].map(m => parseFloat(m[1]));
  raw['Scheduling latency per replica'] = { samplesMs: samples };
  if (samples.length === 0) {
    rows.push({ scenario: 'Scheduling latency per replica', target: '< 50 ms', samples: 0,
      median: '-', worst: '-', status: 'FAIL', notes: 'no "Scheduled replica" log lines found' });
    return;
  }
  const worst = Math.max(...samples);
  rows.push({
    scenario: 'Scheduling latency per replica',
    target: '< 50 ms',
    samples: samples.length,
    median: `${median(samples).toFixed(2)} ms`,
    worst: `${worst.toFixed(2)} ms`,
    status: worst < 50 ? 'PASS' : 'FAIL',
  });
}

// ── Main ──────────────────────────────────────────────────────────────────

function writeResults(started: Date): void {
  fs.mkdirSync(RESULTS_DIR, { recursive: true });
  const meta = { startedAt: started.toISOString(), finishedAt: new Date().toISOString(), iterations: ITERATIONS };
  fs.writeFileSync(path.join(RESULTS_DIR, 'latest.json'), JSON.stringify({ meta, rows, raw }, null, 2));
  const md = [
    `# Portside chaos results`,
    ``,
    `Run ${meta.startedAt} on a 3-node docker-compose cluster, ${ITERATIONS} iterations per timing scenario.`,
    ``,
    `| Scenario | Target | Samples | Median | Worst | Result | Notes |`,
    `| :--- | :--- | ---: | ---: | ---: | :---: | :--- |`,
    ...rows.map(r => `| ${r.scenario} | ${r.target} | ${r.samples} | ${r.median} | ${r.worst} | ${r.status} | ${r.notes || ''} |`),
    ``,
  ].join('\n');
  fs.writeFileSync(path.join(RESULTS_DIR, 'latest.md'), md);
}

async function main(): Promise<void> {
  const started = new Date();
  log('================================================================');
  log('            PORTSIDE CHAOS HARNESS (live cluster)               ');
  log('================================================================');

  await setup();

  const scenarios: [string, () => Promise<void> | void][] = [
    ['container crash', containerCrash],
    ['node failure', nodeFailure],
    ['network partition', networkPartition],
    ['rolling update', rolloutUnderLoad],
    ['control plane restart', controlPlaneRestart],
    ['scheduling latency', schedulingLatency],
  ];
  for (const [name, fn] of scenarios) {
    try {
      await fn();
    } catch (err) {
      rows.push({ scenario: name, target: '-', samples: 0, median: '-', worst: '-', status: 'FAIL',
        notes: `harness error: ${(err as Error).message}` });
      log(`  ${name} aborted: ${(err as Error).message}`);
    }
  }

  log('\n================================================================');
  log('                         RESULTS                                ');
  log('================================================================\n');
  console.table(rows.map(({ notes, ...r }) => r));
  for (const r of rows.filter(r => r.notes)) log(`  ${r.scenario}: ${r.notes}`);
  writeResults(started);
  log(`\nResults written to ${path.relative(process.cwd(), RESULTS_DIR)}/latest.{json,md}`);

  const failed = rows.filter(r => r.status === 'FAIL').length;
  if (failed > 0) {
    log(`\n${failed} scenario(s) FAILED.`);
    process.exitCode = 1;
  } else {
    log('\nAll scenarios passed.');
  }
}

main().catch(err => {
  console.error(`\nChaos harness could not run: ${(err as Error).message}`);
  process.exitCode = 2;
});
