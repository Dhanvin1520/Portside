import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['unit/**/*.test.ts', 'property/**/*.test.ts'],
    alias: {
      '@portside/shared': path.resolve(__dirname, '../shared/src'),
      '@portside/control-plane': path.resolve(__dirname, '../control-plane/src'),
    },
  },
});
