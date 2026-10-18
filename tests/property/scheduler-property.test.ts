import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fc from 'fast-check';
import fs from 'fs';
import path from 'path';
import { StateDB } from '../../control-plane/src/db';
import { Scheduler } from '../../control-plane/src/scheduler';
import { Replica } from '../../shared/src/types';

describe('Scheduler Property-Based Tests (fast-check)', () => {
  let db: StateDB;
  let scheduler: Scheduler;
  const testDbPath = path.join(__dirname, 'test-prop-scheduler.db');

  beforeEach(() => {
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
    db = new StateDB(testDbPath);
    scheduler = new Scheduler(db);
  });

  afterEach(() => {
    db.close();
    if (fs.existsSync(testDbPath)) fs.unlinkSync(testDbPath);
  });

  it('guarantees NO node is ever over-committed on CPU or memory across random workloads', () => {
    fc.assert(
      fc.property(
        // Generate 1-5 nodes with random capacities
        fc.array(
          fc.record({
            name: fc.stringMatching(/node-[a-z0-9]{3}/),
            cpuCapacity: fc.integer({ min: 1000, max: 8000 }),
            memCapacity: fc.integer({ min: 1024, max: 16384 }),
          }),
          { minLength: 1, maxLength: 5 }
        ),
        // Generate 5-20 random replica requests
        fc.array(
          fc.record({
            id: fc.uuid(),
            cpuRequest: fc.integer({ min: 100, max: 2000 }),
            memRequest: fc.integer({ min: 128, max: 4096 }),
          }),
          { minLength: 5, maxLength: 20 }
        ),
        (nodes, replicaSpecs) => {
          // Reset DB tables
          for (const n of db.getAllNodes()) {
            db.updateNodeAllocations(n.name, 0, 0);
          }

          // Register generated nodes
          const uniqueNodes = Array.from(new Map(nodes.map(n => [n.name, n])).values());
          for (const n of uniqueNodes) {
            db.upsertNode({
              name: n.name,
              cpuCapacity: n.cpuCapacity,
              memCapacity: n.memCapacity,
              cpuAllocated: 0,
              memAllocated: 0,
              status: 'Ready',
              lastHeartbeatAt: Date.now(),
            });
          }

          // Schedule each replica
          for (const spec of replicaSpecs) {
            const replica: Replica = {
              id: spec.id,
              deploymentName: 'prop-app',
              revision: 1,
              node: null,
              state: 'Pending',
              restartCount: 0,
              ready: false,
              containerPort: 8080,
              hostPort: 0,
              lastStateChange: Date.now(),
              image: 'test-image',
              cpuRequest: spec.cpuRequest,
              memRequest: spec.memRequest,
            };

            const result = scheduler.schedule(replica);
            if (result.success && result.nodeName) {
              const node = db.getNode(result.nodeName)!;
              // INVARIANT: Allocated must never exceed capacity
              expect(node.cpuAllocated).toBeLessThanOrEqual(node.cpuCapacity);
              expect(node.memAllocated).toBeLessThanOrEqual(node.memCapacity);
            }
          }

          // Final assertion on all nodes
          for (const n of db.getAllNodes()) {
            expect(n.cpuAllocated).toBeLessThanOrEqual(n.cpuCapacity);
            expect(n.memAllocated).toBeLessThanOrEqual(n.memCapacity);
          }
        }
      ),
      { numRuns: 100 }
    );
  });
});
