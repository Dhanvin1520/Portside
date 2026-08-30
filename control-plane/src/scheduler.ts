import { ClusterNode, Replica, createLogger } from '@portside/shared';
import { StateDB } from './db';

const log = createLogger('scheduler');

// ─────────────────────────────────────────────────────────────────────────────
// Resource-Aware Scheduler
//
// Scheduling algorithm:
// 1. Filter: only consider nodes that are Ready (not NotReady or Draining)
// 2. Filter: only nodes with enough free CPU and memory for the replica
// 3. Score: spread replicas across nodes (least-allocated-first)
// 4. Pick the node with the lowest allocation ratio
//
// This is a simplified version of the Kubernetes scheduler's
// filter → score → pick pipeline.
// ─────────────────────────────────────────────────────────────────────────────

export interface ScheduleResult {
  success: boolean;
  nodeName?: string;
  reason?: string;
}

export class Scheduler {
  constructor(private db: StateDB) {}

  /**
   * Schedule a replica onto the best available node.
   * Returns the chosen node name, or a reason if scheduling failed.
   */
  schedule(replica: Replica): ScheduleResult {
    const startTime = performance.now();
    const nodes = this.db.getAllNodes();

    // Phase 1: Filter — only Ready nodes with enough resources
    const feasible = nodes.filter(node => {
      if (node.status !== 'Ready') return false;

      const freeCpu = node.cpuCapacity - node.cpuAllocated;
      const freeMem = node.memCapacity - node.memAllocated;

      return freeCpu >= replica.cpuRequest && freeMem >= replica.memRequest;
    });

    if (feasible.length === 0) {
      const reason = this.buildPendingReason(nodes, replica);
      log.warn('No feasible nodes for replica', {
        replicaId: replica.id,
        cpuRequest: replica.cpuRequest,
        memRequest: replica.memRequest,
        reason,
      });
      return { success: false, reason };
    }

    // Phase 2: Score — least-allocated-first (spread)
    const scored = feasible.map(node => ({
      node,
      score: this.scoreNode(node, replica),
    }));
    scored.sort((a, b) => b.score - a.score); // higher score = better

    const chosen = scored[0].node;
    const elapsed = performance.now() - startTime;

    log.info('Scheduled replica', {
      replicaId: replica.id,
      node: chosen.name,
      latencyMs: elapsed.toFixed(2),
    });

    // Update node allocations
    this.db.updateNodeAllocations(
      chosen.name,
      chosen.cpuAllocated + replica.cpuRequest,
      chosen.memAllocated + replica.memRequest
    );

    return { success: true, nodeName: chosen.name };
  }

  /**
   * Release resources when a replica is removed from a node.
   */
  release(replica: Replica): void {
    if (!replica.node) return;

    const node = this.db.getNode(replica.node);
    if (!node) return;

    this.db.updateNodeAllocations(
      node.name,
      Math.max(0, node.cpuAllocated - replica.cpuRequest),
      Math.max(0, node.memAllocated - replica.memRequest)
    );
  }

  /**
   * Recalculate node allocations from actual replica placement.
   * Used during control plane recovery.
   */
  recalculateAllocations(): void {
    const nodes = this.db.getAllNodes();
    for (const node of nodes) {
      const replicas = this.db.getReplicasOnNode(node.name);
      const activeReplicas = replicas.filter(
        r => !['Terminated', 'Terminating'].includes(r.state)
      );
      const cpuAllocated = activeReplicas.reduce((sum, r) => sum + r.cpuRequest, 0);
      const memAllocated = activeReplicas.reduce((sum, r) => sum + r.memRequest, 0);
      this.db.updateNodeAllocations(node.name, cpuAllocated, memAllocated);
    }
    log.info('Recalculated node allocations');
  }

  // ── Scoring ──────────────────────────────────────────────────────────────

  /**
   * Score a node for a replica. Higher is better.
   * Uses Least-Allocated strategy to spread replicas across nodes.
   */
  private scoreNode(node: ClusterNode, replica: Replica): number {
    const cpuFree = (node.cpuCapacity - node.cpuAllocated) / node.cpuCapacity;
    const memFree = (node.memCapacity - node.memAllocated) / node.memCapacity;

    // Weighted average: CPU 50%, Memory 50%
    const spreadScore = (cpuFree + memFree) / 2;

    // Anti-affinity: slight penalty for having replicas of the same deployment
    const sameDepReplicas = this.db.getReplicasOnNode(node.name)
      .filter(r => r.deploymentName === replica.deploymentName && r.state !== 'Terminated')
      .length;
    const antiAffinityBonus = 1 / (1 + sameDepReplicas);

    return spreadScore * 0.7 + antiAffinityBonus * 0.3;
  }

  private buildPendingReason(nodes: ClusterNode[], replica: Replica): string {
    const readyNodes = nodes.filter(n => n.status === 'Ready');
    if (readyNodes.length === 0) {
      return 'No Ready nodes in cluster';
    }

    const insufficientCpu = readyNodes.filter(
      n => (n.cpuCapacity - n.cpuAllocated) < replica.cpuRequest
    ).length;
    const insufficientMem = readyNodes.filter(
      n => (n.memCapacity - n.memAllocated) < replica.memRequest
    ).length;

    const reasons: string[] = [];
    if (insufficientCpu > 0) reasons.push(`${insufficientCpu} node(s) have insufficient CPU`);
    if (insufficientMem > 0) reasons.push(`${insufficientMem} node(s) have insufficient memory`);
    return reasons.join('; ') || 'No feasible nodes';
  }
}
