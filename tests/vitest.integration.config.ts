import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['integration/**/*.test.ts'],
    testTimeout: 60000,
    env: { LOG_LEVEL: 'error' },
    alias: {
      '@portside/shared': path.resolve(__dirname, '../shared/src'),
      '@portside/control-plane': path.resolve(__dirname, '../control-plane/src'),
    },
  },
});
