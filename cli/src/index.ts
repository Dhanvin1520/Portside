#!/usr/bin/env node
import { Command } from 'commander';
import fs from 'fs';
import chalk from 'chalk';
import Table from 'cli-table3';

// ─────────────────────────────────────────────────────────────────────────────
// portctl — CLI for Portside Container Orchestrator
//
// Usage:
//   portctl apply -f deployment.yaml    Apply a Deployment or Service
//   portctl get deployments             List all deployments
//   portctl get replicas                List all replicas
//   portctl get nodes                   List all nodes
//   portctl get events                  List recent events
//   portctl scale <name> --replicas N   Scale a deployment
//   portctl rollout undo <name>         Roll back a deployment
//   portctl drain <node>                Drain a node
//   portctl describe <name>             Show deployment details and events
// ─────────────────────────────────────────────────────────────────────────────

const API_BASE = process.env.PORTSIDE_API || 'http://localhost:7000';

const program = new Command();
program
  .name('portctl')
  .description('CLI for Portside container orchestrator')
  .version('1.0.0');

// ── Apply ──────────────────────────────────────────────────────────────

program
  .command('apply')
  .description('Apply a Deployment or Service from YAML')
  .requiredOption('-f, --file <path>', 'Path to YAML file')
  .action(async (opts) => {
    try {
      const content = fs.readFileSync(opts.file, 'utf8');
      const res = await fetch(`${API_BASE}/v1/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-yaml' },
        body: content,
      });

      const data = (await res.json()) as any;
      if (!res.ok) {
        console.error(chalk.red(`Error: ${data.error}`));
        process.exit(1);
      }

      for (const item of data.applied) {
        console.log(chalk.green(`${item.kind}/${item.name} ${item.action}`));
      }
    } catch (err) {
      console.error(chalk.red(`Failed to connect: ${(err as Error).message}`));
      process.exit(1);
    }
  });

// ── Get ────────────────────────────────────────────────────────────────

const get = program.command('get').description('Display resources');

get.command('deployments')
  .alias('deploy')
  .description('List all deployments')
  .action(async () => {
    const data = await apiGet('/v1/deployments');
    const table = new Table({
      head: ['NAME', 'REPLICAS', 'READY', 'REVISION', 'STRATEGY'],
      style: { head: ['cyan'] },
    });

    for (const d of data) {
      table.push([
        d.name,
        `${d.activeReplicas}/${d.desiredReplicas}`,
        String(d.readyReplicas),
        String(d.currentRevision),
        `maxSurge=${d.strategy.maxSurge}, maxUnavail=${d.strategy.maxUnavailable}`,
      ]);
    }
    console.log(table.toString());
  });

get.command('replicas')
  .alias('pods')
  .description('List all replicas')
  .option('-d, --deployment <name>', 'Filter by deployment')
  .action(async (opts) => {
    const query = opts.deployment ? `?deployment=${opts.deployment}` : '';
    const data = await apiGet(`/v1/replicas${query}`);
    const table = new Table({
      head: ['ID', 'DEPLOYMENT', 'NODE', 'STATE', 'READY', 'RESTARTS', 'PORT', 'REV'],
      style: { head: ['cyan'] },
    });

    for (const r of data) {
      const stateColor = r.ready ? chalk.green : r.state === 'Pending' ? chalk.yellow : chalk.red;
      table.push([
        r.id,
        r.deploymentName,
        r.node || '-',
        stateColor(r.state),
        r.ready ? chalk.green('✓') : chalk.red('✗'),
        String(r.restartCount),
        r.hostPort || '-',
        String(r.revision),
      ]);
    }
    console.log(table.toString());
  });

get.command('nodes')
  .description('List all cluster nodes')
  .action(async () => {
    const data = await apiGet('/v1/nodes');
    const table = new Table({
      head: ['NAME', 'STATUS', 'CPU', 'MEMORY', 'REPLICAS'],
      style: { head: ['cyan'] },
    });

    for (const n of data) {
      const statusColor = n.status === 'Ready' ? chalk.green :
        n.status === 'Draining' ? chalk.yellow : chalk.red;
      table.push([
        n.name,
        statusColor(n.status),
        `${n.cpuAllocated}/${n.cpuCapacity}m (${n.cpuUtilization})`,
        `${n.memAllocated}/${n.memCapacity}Mi (${n.memUtilization})`,
        String(n.replicaCount),
      ]);
    }
    console.log(table.toString());
  });

get.command('events')
  .description('List recent cluster events')
  .option('-n, --limit <count>', 'Number of events', '20')
  .option('-o, --object <name>', 'Filter by object')
  .action(async (opts) => {
    const params = new URLSearchParams();
    if (opts.limit) params.set('limit', opts.limit);
    if (opts.object) params.set('object', opts.object);
    const data = await apiGet(`/v1/events?${params}`);
    const table = new Table({
      head: ['TIME', 'KIND', 'OBJECT', 'MESSAGE'],
      style: { head: ['cyan'] },
    });

    for (const e of data) {
      table.push([
        new Date(e.time).toLocaleTimeString(),
        e.kind,
        e.object,
        e.message,
      ]);
    }
    console.log(table.toString());
  });

// ── Scale ──────────────────────────────────────────────────────────────

program
  .command('scale <deployment>')
  .description('Scale a deployment')
  .requiredOption('--replicas <count>', 'Desired replica count')
  .action(async (deployment, opts) => {
    try {
      const res = await fetch(`${API_BASE}/v1/deployments/${deployment}/scale`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ replicas: parseInt(opts.replicas, 10) }),
      });

      const data = (await res.json()) as any;
      if (!res.ok) {
        console.error(chalk.red(`Error: ${data.error}`));
        process.exit(1);
      }
      console.log(chalk.green(data.message));
    } catch (err) {
      console.error(chalk.red(`Failed: ${(err as Error).message}`));
      process.exit(1);
    }
  });

// ── Rollout ────────────────────────────────────────────────────────────

const rollout = program.command('rollout').description('Manage rollouts');

rollout
  .command('undo <deployment>')
  .description('Roll back to the previous revision')
  .action(async (deployment) => {
    try {
      const res = await fetch(`${API_BASE}/v1/deployments/${deployment}/rollback`, {
        method: 'POST',
      });

      const data = (await res.json()) as any;
      if (!res.ok) {
        console.error(chalk.red(`Error: ${data.error}`));
        process.exit(1);
      }
      console.log(chalk.green(data.message));
    } catch (err) {
      console.error(chalk.red(`Failed: ${(err as Error).message}`));
      process.exit(1);
    }
  });

// ── Drain ──────────────────────────────────────────────────────────────

program
  .command('drain <node>')
  .description('Drain replicas from a node')
  .action(async (node) => {
    try {
      const res = await fetch(`${API_BASE}/v1/nodes/${node}/drain`, {
        method: 'POST',
      });

      const data = (await res.json()) as any;
      if (!res.ok) {
        console.error(chalk.red(`Error: ${data.error}`));
        process.exit(1);
      }
      console.log(chalk.green(data.message));
    } catch (err) {
      console.error(chalk.red(`Failed: ${(err as Error).message}`));
      process.exit(1);
    }
  });

// ── Describe ───────────────────────────────────────────────────────────

program
  .command('describe <deployment>')
  .description('Show deployment details and event timeline')
  .action(async (deployment) => {
    try {
      const dep = await apiGet(`/v1/deployments/${deployment}`);

      console.log(chalk.bold.cyan(`\nDeployment: ${dep.name}`));
      console.log(`  Revision:   ${dep.currentRevision}`);
      console.log(`  Replicas:   ${dep.activeReplicas}/${dep.desiredReplicas} (${dep.readyReplicas} ready)`);
      console.log(`  Strategy:   maxSurge=${dep.strategy.maxSurge}, maxUnavailable=${dep.strategy.maxUnavailable}`);

      // Replicas
      const replicas = await apiGet(`/v1/replicas?deployment=${deployment}`);
      console.log(chalk.bold('\n  Replicas:'));
      for (const r of replicas) {
        const icon = r.ready ? chalk.green('●') : chalk.red('●');
        console.log(`    ${icon} ${r.id}  node=${r.node || '-'}  state=${r.state}  restarts=${r.restartCount}`);
      }

      // Events
      const events = await apiGet(`/v1/events?object=deployment/${deployment}&limit=10`);
      console.log(chalk.bold('\n  Events:'));
      for (const e of events) {
        console.log(`    ${new Date(e.time).toLocaleTimeString()}  ${e.kind}  ${e.message}`);
      }
      console.log('');
    } catch (err) {
      console.error(chalk.red(`Failed: ${(err as Error).message}`));
      process.exit(1);
    }
  });

// ── Helper ─────────────────────────────────────────────────────────────

async function apiGet(path: string): Promise<any> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const data = (await res.json().catch(() => ({ error: res.statusText }))) as any;
    throw new Error(data.error || res.statusText);
  }
  return (await res.json()) as any;
}

program.parse();
