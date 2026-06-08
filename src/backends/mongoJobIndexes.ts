/**
 * Index definitions for the MongoJobQueue collection.
 * Extracted so MongoJobQueue.ts stays focused.
 */
import type { Collection } from 'mongodb'

import type { JobDoc } from '../types'

export async function createJobIndexes(
  collection: Collection<JobDoc>,
): Promise<void> {
  await collection.createIndex(
    { type: 1, status: 1, runAt: 1, priority: 1 },
    { name: 'claim_next_idx' },
  )
  await collection.createIndex({ type: 1, status: 1 }, { name: 'stats_idx' })
  await collection.createIndex(
    { dedupeKey: 1, dedupeScope: 1 },
    {
      name: 'dedupe_pending_active_idx',
      unique: true,
      partialFilterExpression: {
        dedupeKey: { $exists: true },
        dedupeScope: 'pending+active',
        status: { $in: ['pending', 'active'] },
      },
    },
  )
  await collection.createIndex(
    { dedupeKey: 1, dedupeScope: 1 },
    {
      name: 'dedupe_pending_idx',
      unique: true,
      partialFilterExpression: {
        dedupeKey: { $exists: true },
        dedupeScope: 'pending',
        status: 'pending',
      },
    },
  )
  await collection.createIndex(
    { status: 1, claimedAt: 1 },
    { name: 'visibility_timeout_idx' },
  )
}
