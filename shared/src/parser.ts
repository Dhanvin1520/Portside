import * as yaml from 'yaml';
import { DeploymentSpec, ServiceSpec, PortsideSpec } from './types';

// ─────────────────────────────────────────────────────────────────────────────
// YAML Parser & Validator
// Parses multi-document YAML and validates each document as a Portside spec
// ─────────────────────────────────────────────────────────────────────────────

export class SpecValidationError extends Error {
  constructor(public readonly errors: string[]) {
    super(`Invalid spec:\n${errors.map(e => `  - ${e}`).join('\n')}`);
    this.name = 'SpecValidationError';
  }
}

export function parseSpecs(yamlContent: string): PortsideSpec[] {
  const docs = yaml.parseAllDocuments(yamlContent);
  const specs: PortsideSpec[] = [];

  for (const doc of docs) {
    if (doc.errors.length > 0) {
      throw new SpecValidationError(
        doc.errors.map(e => `YAML parse error: ${e.message}`)
      );
    }
    const obj = doc.toJSON();
    if (!obj) continue;

    validateSpec(obj);
    specs.push(obj as PortsideSpec);
  }

  if (specs.length === 0) {
    throw new SpecValidationError(['No valid documents found in YAML']);
  }
  return specs;
}

function validateSpec(obj: any): void {
  const errors: string[] = [];

  if (!obj.kind) {
    errors.push('Missing required field: kind');
  }

  if (!obj.metadata?.name) {
    errors.push('Missing required field: metadata.name');
  }

  if (typeof obj.metadata?.name === 'string' && !/^[a-z0-9][a-z0-9-]*$/.test(obj.metadata.name)) {
    errors.push(`Invalid name "${obj.metadata.name}": must match /^[a-z0-9][a-z0-9-]*$/`);
  }

  if (obj.kind === 'Deployment') {
    validateDeployment(obj, errors);
  } else if (obj.kind === 'Service') {
    validateService(obj, errors);
  } else {
    errors.push(`Unknown kind: ${obj.kind}. Supported: Deployment, Service`);
  }

  if (errors.length > 0) {
    throw new SpecValidationError(errors);
  }
}

function validateDeployment(obj: any, errors: string[]): void {
  if (!obj.spec) {
    errors.push('Deployment missing spec');
    return;
  }

  if (typeof obj.spec.replicas !== 'number' || obj.spec.replicas < 0) {
    errors.push('spec.replicas must be a non-negative integer');
  }

  if (!obj.spec.selector?.matchLabels || Object.keys(obj.spec.selector.matchLabels).length === 0) {
    errors.push('spec.selector.matchLabels must have at least one label');
  }

  if (!obj.spec.template?.spec?.containers || obj.spec.template.spec.containers.length === 0) {
    errors.push('spec.template.spec.containers must have at least one container');
  }

  const containers = obj.spec.template?.spec?.containers || [];
  for (const c of containers) {
    if (!c.name) errors.push('Container missing name');
    if (!c.image) errors.push('Container missing image');
  }

  // Validate strategy if present
  if (obj.spec.strategy) {
    const s = obj.spec.strategy;
    if (s.type && s.type !== 'RollingUpdate') {
      errors.push(`Unsupported strategy type: ${s.type}`);
    }
    if (s.rollingUpdate) {
      if (s.rollingUpdate.maxSurge !== undefined && (typeof s.rollingUpdate.maxSurge !== 'number' || s.rollingUpdate.maxSurge < 0)) {
        errors.push('strategy.rollingUpdate.maxSurge must be a non-negative integer');
      }
      if (s.rollingUpdate.maxUnavailable !== undefined && (typeof s.rollingUpdate.maxUnavailable !== 'number' || s.rollingUpdate.maxUnavailable < 0)) {
        errors.push('strategy.rollingUpdate.maxUnavailable must be a non-negative integer');
      }
    }
  }
}

function validateService(obj: any, errors: string[]): void {
  if (!obj.spec) {
    errors.push('Service missing spec');
    return;
  }

  if (!obj.spec.selector || Object.keys(obj.spec.selector).length === 0) {
    errors.push('Service spec.selector must have at least one label');
  }

  if (!obj.spec.ports || obj.spec.ports.length === 0) {
    errors.push('Service spec.ports must have at least one port mapping');
  }

  for (const p of obj.spec.ports || []) {
    if (!p.port) errors.push('Port missing port number');
    if (!p.targetPort) errors.push('Port missing targetPort');
  }
}

// ── Resource parsing ─────────────────────────────────────────────────────────

/** Parse CPU string like "500m" or "1" to millicores */
export function parseCpuString(cpu: string | undefined): number {
  if (!cpu) return 100; // default 100m
  if (cpu.endsWith('m')) return parseInt(cpu.slice(0, -1), 10);
  return parseFloat(cpu) * 1000;
}

/** Parse memory string like "256Mi" or "1Gi" to MiB */
export function parseMemoryString(mem: string | undefined): number {
  if (!mem) return 128; // default 128Mi
  if (mem.endsWith('Gi')) return parseFloat(mem.slice(0, -2)) * 1024;
  if (mem.endsWith('Mi')) return parseFloat(mem.slice(0, -2));
  if (mem.endsWith('Ki')) return parseFloat(mem.slice(0, -2)) / 1024;
  return parseInt(mem, 10);
}
