/**
 * Index definitions for the MongoJobQueue collection.
 * Extracted so MongoJobQueue.ts stays focused.
 */
import type { Collection, IndexSpecification } from 'mongodb'

import type { JobDoc } from '../types'

/**
 * Indexes that earlier versions created and that are now superseded. Dropped
 * *after* their replacement exists, so a live deployment never runs a moment
 * without a usable index on the claim path.
 */
const LEGACY_INDEX_NAMES = ['claim_next_idx', 'cleanup_failed_idx']

export async function createJobIndexes(
  collection: Collection<JobDoc>,
): Promise<void> {
  /**
   * Claim path — the hottest query in the system.
   *
   * `claimNext` filters `{ type, status: 'pending', runAt: { $lte: now } }` and
   * sorts `{ priority: 1, runAt: 1 }`. The key order below is load-bearing:
   * equality fields first, then the sort keys **in sort order**. Put `runAt`
   * ahead of `priority` (as the original `claim_next_idx` did) and the index
   * can no longer serve the sort — Mongo scans every matching key and adds a
   * blocking SORT stage, so claim cost grows linearly with backlog depth and a
   * deep enough backlog fails the claim outright on the 32MB sort limit.
   *
   * Measured, 500 pending jobs of one type, single claim:
   *   { type, status, runAt, priority } → 500 keys examined, SORT stage
   *   { type, status, priority, runAt } →   1 key  examined, no SORT
   *
   * The `runAt` range is evaluated as a bound on the trailing key.
   */
  await collection.createIndex(
    { type: 1, status: 1, priority: 1, runAt: 1 },
    { name: 'claim_next_v2_idx' },
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
  /**
   * The `'pending'` dedupe scope means "at most one pending **and** at most one
   * active" — single-flight coalescing. `dedupe_pending_idx` above only
   * constrains the pending half; without this second index nothing stops N
   * concurrent `claimOrEnqueue` calls from each inserting an `active` doc under
   * the same key, which is exactly the mutual exclusion that scope promises.
   *
   * Two separate partial indexes rather than one: a unique index over
   * `status: { $in: ['pending', 'active'] }` would forbid the pending-behind-
   * active pair that the scope exists to allow.
   */
  await collection.createIndex(
    { dedupeKey: 1, dedupeScope: 1 },
    {
      name: 'dedupe_active_idx',
      unique: true,
      partialFilterExpression: {
        dedupeKey: { $exists: true },
        dedupeScope: 'pending',
        status: 'active',
      },
    },
  )
  await collection.createIndex(
    { status: 1, claimedAt: 1 },
    { name: 'visibility_timeout_idx' },
  )
  /**
   * Retention sweep (`cleanupOldJobs`) filters completed jobs by `completedAt`
   * and failed/superseded jobs by `failedAt`. A single compound index cannot
   * serve the `$or`; Mongo needs one index per branch to plan an index union
   * instead of scanning every terminal document. Partial filters keep both
   * indexes off the hot pending/active working set.
   */
  await collection.createIndex(
    { completedAt: 1 },
    {
      name: 'cleanup_completed_idx',
      partialFilterExpression: { status: 'completed' },
    },
  )
  await collection.createIndex(
    { failedAt: 1 },
    {
      name: 'cleanup_failed_v2_idx',
      partialFilterExpression: {
        status: { $in: ['failed', 'superseded'] },
      },
    },
  )

  await dropLegacyIndexes(collection)
}

/**
 * Drop superseded indexes, ignoring "index not found" (fresh collections, and
 * every startup after the first). Any other error is real — a permissions
 * problem, say — and must not be swallowed.
 */
async function dropLegacyIndexes(
  collection: Collection<JobDoc>,
): Promise<void> {
  for (const name of LEGACY_INDEX_NAMES) {
    try {
      await collection.dropIndex(name)
    } catch (err) {
      if (isIndexNotFoundError(err)) continue
      throw err
    }
  }
}

/** IndexNotFound (code 27), matched across driver/server phrasings. */
function isIndexNotFoundError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false
  const e = err as { code?: unknown; codeName?: unknown; message?: string }
  return (
    e.code === 27 ||
    e.codeName === 'IndexNotFound' ||
    (e.message?.includes('index not found') ?? false)
  )
}

/**
 * Index specs the collection is expected to carry after {@link createJobIndexes}.
 * Exported for tests so an assertion about the claim plan can name the index it
 * depends on rather than duplicating the key order.
 */
export const CLAIM_INDEX_NAME = 'claim_next_v2_idx'
export const CLAIM_INDEX_KEY: IndexSpecification = {
  type: 1,
  status: 1,
  priority: 1,
  runAt: 1,
}
