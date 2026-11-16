import { createLogger } from '@portside/shared';
import { Agent } from './agent';
import { ContainerManager } from './container-manager';

const log = createLogger('agent');

// ─────────────────────────────────────────────────────────────────────────────
// Worker agent entry point: wires environment configuration, the Docker
// runtime and the Agent connection loop together.
// ─────────────────────────────────────────────────────────────────────────────

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

  const agent = new Agent(
    {
      nodeName: NODE_NAME,
      controlPlane: CONTROL_PLANE,
      cpuCapacity: CPU_CAPACITY,
      memCapacity: MEM_CAPACITY,
    },
    new ContainerManager()
  );
  await agent.start();

  const shutdown = async () => {
    log.info('Shutting down agent...');
    await agent.stop({ stopContainers: true });
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch(err => {
  log.error('Fatal error', { error: err.message });
  process.exit(1);
});
