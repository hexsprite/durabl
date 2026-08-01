/**
 * Index/query-plan regressions.
 *
 * These assert on the *plan*, not on wall-clock time — a timing assertion for
 * "the claim is fast" is flaky on CI, but "the claim does not blocking-sort" is
 * exactly the property that was broken and is cheap to check.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import { CLAIM_INDEX_NAME } from '../src/backends/mongoJobIndexes'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

/**
 * Does the plan Mongo actually chose contain a blocking in-memory sort?
 *
 * Deliberately inspects the *winning* stages only. A whole-explain string match
 * also catches `rejectedPlans`, which routinely contain SORT stages precisely
 * because the planner discarded them — that would make this assertion pass or
 * fail for reasons unrelated to the index under test.
 */
function hasBlockingSort(explain: {
  executionStats?: { executionStages?: unknown }
  queryPlanner?: { winningPlan?: unknown }
}): boolean {
  const winning =
    explain.executionStats?.executionStages ?? explain.queryPlanner?.winningPlan
  return JSON.stringify(winning ?? {}).includes('"stage":"SORT"')
}

describe('job collection indexes', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>
  let collectionName: string

  beforeEach(async () => {
    ;({ db } = await getMongo())
    collectionName = uniqueCollectionName('index_jobs')
    backend = new MongoJobQueue({ db, collectionName })
    collection = backend.getCollection()
    await backend.startup()
  })

  afterEach(async () => {
    await backend.shutdown()
    await collection.drop().catch(() => {
      /* collection may already be gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  describe('claim path', () => {
    // Regression: the index was { type, status, runAt, priority } while the
    // claim sorts { priority, runAt }. With the sort key behind the range key
    // the index cannot serve the sort, so Mongo scanned every pending job of
    // the type and added a blocking SORT — claim cost grew with backlog depth,
    // and a deep enough backlog fails outright on the 32MB sort limit.
    it('serves the claim sort from the index instead of blocking-sorting the backlog', async () => {
      for (let i = 0; i < 300; i++) {
        await backend.enqueue('bulk', { i }, { priority: i % 5 })
      }

      const explain = await collection
        .find({ type: 'bulk', status: 'pending', runAt: { $lte: new Date() } })
        .sort({ priority: 1, runAt: 1 })
        .limit(1)
        .explain('executionStats')

      const stats = explain.executionStats as {
        totalKeysExamined: number
        nReturned: number
      }

      expect(hasBlockingSort(explain)).toBe(false)
      // One key per returned doc — not one per job in the backlog.
      expect(stats.totalKeysExamined).toBeLessThanOrEqual(stats.nReturned + 1)
      expect(JSON.stringify(explain)).toContain(CLAIM_INDEX_NAME)
    })

    it('still claims in priority then runAt order', async () => {
      await backend.enqueue('ordered', { n: 'low' }, { priority: 5 })
      await backend.enqueue('ordered', { n: 'high' }, { priority: 0 })
      await backend.enqueue('ordered', { n: 'mid' }, { priority: 1 })

      expect((await backend.claimNext('ordered'))!.data).toEqual({ n: 'high' })
      expect((await backend.claimNext('ordered'))!.data).toEqual({ n: 'mid' })
      expect((await backend.claimNext('ordered'))!.data).toEqual({ n: 'low' })
    })
  })

  describe('startup migration', () => {
    it('replaces a legacy claim_next_idx and leaves no window without a claim index', async () => {
      const legacyName = 'claim_next_idx'
      const legacy = db.collection<JobDoc>(uniqueCollectionName('legacy_jobs'))
      // Recreate the pre-fix state: the old key order under the old name.
      await legacy.createIndex(
        { type: 1, status: 1, runAt: 1, priority: 1 },
        { name: legacyName },
      )

      const migrated = new MongoJobQueue({
        db,
        collectionName: legacy.collectionName,
      })
      await migrated.startup()

      const names = (await legacy.listIndexes().toArray()).map((i) => i.name)
      expect(names).toContain(CLAIM_INDEX_NAME)
      expect(names).not.toContain(legacyName)

      await migrated.shutdown()
      await legacy.drop().catch(() => {
        /* already gone */
      })
    })

    it('is idempotent across repeated startups', async () => {
      await backend.startup()
      await backend.startup()

      const names = (await collection.listIndexes().toArray()).map((i) => i.name)
      expect(names).toContain(CLAIM_INDEX_NAME)
      expect(new Set(names).size).toBe(names.length)
    })
  })

  describe('retention sweep', () => {
    // Regression: nothing indexed completedAt/failedAt, so cleanupOldJobs
    // scanned every terminal document — most of a busy collection.
    it('indexes both branches of the cleanup $or', async () => {
      const names = (await collection.listIndexes().toArray()).map((i) => i.name)
      expect(names).toContain('cleanup_completed_idx')
      expect(names).toContain('cleanup_failed_idx')
    })

    it('plans the cleanup filter without a collection scan', async () => {
      for (let i = 0; i < 50; i++) {
        const id = await backend.enqueue('old', { i })
        await backend.claimNext('old')
        await backend.complete(id!)
      }

      const cutoff = new Date(Date.now() + 60_000) // everything is "old"
      const explain = await collection
        .find({
          status: { $in: ['completed', 'failed'] },
          $or: [{ completedAt: { $lt: cutoff } }, { failedAt: { $lt: cutoff } }],
        })
        .explain('queryPlanner')

      expect(JSON.stringify(explain)).not.toContain('"stage":"COLLSCAN"')
    })

    it('still deletes exactly the aged terminal jobs', async () => {
      const keep = await backend.enqueue('keep', {})
      const drop = await backend.enqueue('drop', {})
      await backend.claimNext('drop')
      await backend.complete(drop!)

      const removed = await backend.cleanupOldJobs(-1) // cutoff in the future
      expect(removed).toBe(1)
      expect(await collection.findOne({ _id: keep! })).not.toBeNull()
      expect(await collection.findOne({ _id: drop! })).toBeNull()
    })
  })
})
