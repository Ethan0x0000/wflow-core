import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/temporal.test.ts'],
    retry: 2,
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    fileParallelism: false,
    testTimeout: 60_000,
    hookTimeout: 30_000,
  },
});
