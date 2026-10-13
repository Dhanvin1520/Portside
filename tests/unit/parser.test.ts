import { describe, it, expect } from 'vitest';
import { parseSpecs, parseCpuString, parseMemoryString, SpecValidationError } from '../../shared/src/parser';

describe('YAML Spec Parser & Validator', () => {
  it('parses valid multi-document deployment and service specs', () => {
    const yaml = `
apiVersion: portside/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 3
  selector:
    matchLabels:
      app: web
  template:
    spec:
      containers:
        - name: web
          image: nginx:alpine
          resources:
            requests:
              cpu: 500m
              memory: 256Mi
---
apiVersion: portside/v1
kind: Service
metadata:
  name: web-service
spec:
  selector:
    app: web
  ports:
    - port: 80
      targetPort: 8080
`;

    const specs = parseSpecs(yaml);
    expect(specs).toHaveLength(2);
    expect(specs[0].kind).toBe('Deployment');
    expect(specs[1].kind).toBe('Service');
  });

  it('rejects deployment missing container specs', () => {
    const badYaml = `
apiVersion: portside/v1
kind: Deployment
metadata:
  name: bad-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bad
  template:
    spec:
      containers: []
`;

    expect(() => parseSpecs(badYaml)).toThrowError(SpecValidationError);
  });

  it('rejects invalid names with uppercase or symbols', () => {
    const badYaml = `
apiVersion: portside/v1
kind: Deployment
metadata:
  name: BAD_NAME!
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bad
  template:
    spec:
      containers:
        - name: c1
          image: alpine
`;

    expect(() => parseSpecs(badYaml)).toThrowError(/Invalid name/);
  });

  it('rejects negative replica counts', () => {
    const badYaml = `
apiVersion: portside/v1
kind: Deployment
metadata:
  name: bad-app
spec:
  replicas: -5
  selector:
    matchLabels:
      app: bad
  template:
    spec:
      containers:
        - name: c1
          image: alpine
`;

    expect(() => parseSpecs(badYaml)).toThrowError(/non-negative/);
  });

  it('accurately parses CPU millicores and Memory MiB representations', () => {
    expect(parseCpuString('500m')).toBe(500);
    expect(parseCpuString('1')).toBe(1000);
    expect(parseCpuString('2.5')).toBe(2500);
    expect(parseCpuString(undefined)).toBe(100);

    expect(parseMemoryString('512Mi')).toBe(512);
    expect(parseMemoryString('2Gi')).toBe(2048);
    expect(parseMemoryString('1024Ki')).toBe(1);
    expect(parseMemoryString(undefined)).toBe(128);
  });
});
