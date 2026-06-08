import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // Mongo-backed suites spin up an in-memory replica set (or connect to
    // MONGO_URL); give them room and run serially to avoid oplog contention.
    testTimeout: 30000,
    hookTimeout: 60000,
    fileParallelism: false,
    include: ['test/**/*.test.ts'],
  },
})
