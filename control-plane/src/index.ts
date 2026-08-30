import { createLogger } from '@portside/shared';
import { StateDB } from './db';
import { Scheduler } from './scheduler';
import { Reconciler } from './reconciler';
import { GrpcServer } from './grpc-server';
import { createRestApi } from './api';

const log = createLogger('control-plane');

// ─────────────────────────────────────────────────────────────────────────────
// Control Plane Entry Point
//
// Starts all subsystems:
// 1. SQLite database (state persistence)
// 2. Scheduler (resource-aware placement)
// 3. Reconciler (level-triggered convergence loop)
// 4. gRPC server (agent communication)
// 5. REST API (CLI and dashboard)
//
// On restart, recovers state from SQLite and reconciles with agent reports.
// ─────────────────────────────────────────────────────────────────────────────

const REST_PORT = parseInt(process.env.REST_PORT || '7000', 10);
const GRPC_PORT = parseInt(process.env.GRPC_PORT || '7001', 10);
const DATA_DIR = process.env.DATA_DIR || './data';

async function main(): Promise<void> {
  log.info('Starting Portside control plane', {
    restPort: REST_PORT,
    grpcPort: GRPC_PORT,
    dataDir: DATA_DIR,
  });

  // Initialize subsystems
  const db = new StateDB(`${DATA_DIR}/portside.db`);
  const scheduler = new Scheduler(db);
  const reconciler = new Reconciler(db, scheduler);
  const grpcServer = new GrpcServer(db, reconciler, GRPC_PORT);
  const app = createRestApi(db, reconciler, scheduler);

  // Recovery: recalculate allocations from persisted state
  scheduler.recalculateAllocations();
  log.info('State recovered from database');

  // Start gRPC server (for agent communication)
  await grpcServer.start();

  // Start reconcile loop
  reconciler.start();

  // Start REST API
  app.listen(REST_PORT, () => {
    log.info(`REST API listening on port ${REST_PORT}`);
  });

  // Graceful shutdown
  const shutdown = () => {
    log.info('Shutting down...');
    reconciler.stop();
    grpcServer.stop();
    db.close();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch(err => {
  log.error('Fatal error', { error: err.message, stack: err.stack });
  process.exit(1);
});
