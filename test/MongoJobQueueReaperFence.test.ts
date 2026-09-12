/**
 * Reaper lease fencing.
 *
 * Symptom: the sweep read a batch of expired leases, then wrote each one
 * back to `pending` (or terminal) with a filter that no longer checked the
 * lease. A worker that woke and heartbeated mid-sweep still lost its active
 * run to a second worker — a double execution. The recovery filter must
 * re-assert `claimedAt < cutoff` at write time, and the returned count must
 * only include writes that applied.
 */
import type { Collection, Db, UpdateFilter } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue reaper lease fencing', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  const visibilityTimeoutMs = 300_000
  const staleClaimedAt = new Date(Date.now() - visibilityTimeoutMs - 60_000)

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('reaper_fence_jobs'),
    })
    collection = backend.getCollection()
    await backend.startup()
  })

  afterEach(async () => {
    await backend.shutdown()
    await collection.drop().catch(() => {
      /* already gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it('a heartbeat that lands mid-sweep keeps the active run', async () => {
    // Two stalled jobs, A claimed longer ago than B, so the sweep (sorted
    // oldest-lease-first) visits A before B.
    await backend.enqueue('t', { n: 'a' })
    await backend.enqueue('t', { n: 'b' })
    const jobA = await backend.claimNext<{ n: string }>('t')
    const jobB = await backend.claimNext<{ n: string }>('t')
    expect(jobA).not.toBeNull()
    expect(jobB).not.toBeNull()

    await collection.updateOne(
      { _id: jobA!.id },
      { $set: { claimedAt: staleClaimedAt } },
    )
    await collection.updateOne(
      { _id: jobB!.id },
      { $set: { claimedAt: new Date(staleClaimedAt.getTime() + 1) } },
    )

    // Simulate B's owning worker waking and heartbeating between the sweep's
    // cursor read and its recovery write for A. The first `updateOne` inside
    // `recoverStuckJobs` is A's recovery write (A sorts first); we hijack
    // that moment to land B's heartbeat before the sweep ever reaches B.
    const originalUpdateOne = collection.updateOne.bind(collection)
    const updateOneSpy = vi.spyOn(collection, 'updateOne')
    let heartbeated = false
    updateOneSpy.mockImplementation(async (filter, update, options) => {
      if (!heartbeated) {
        heartbeated = true
        await backend.heartbeat(jobB!.id, jobB!.claimToken)
      }
      return originalUpdateOne(
        filter,
        update as UpdateFilter<JobDoc>,
        options,
      )
    })

    try {
      const handled = await backend.recoverStuckJobs(visibilityTimeoutMs)

      // Only A's recovery applied. B's heartbeat refreshed its lease before
      // the sweep's write for B, so the `claimedAt: { $lt: cutoff }` guard
      // must exclude it — without the fix this write still applied and B's
      // active run was handed to a second worker while the first was still
      // running.
      expect(handled).toBe(1)
    } finally {
      updateOneSpy.mockRestore()
    }

    const recoveredA = await collection.findOne({ _id: jobA!.id })
    expect(recoveredA?.status).toBe('pending')

    const stillActiveB = await collection.findOne({ _id: jobB!.id })
    expect(stillActiveB?.status).toBe('active')
    expect(stillActiveB?.claimToken).toBe(jobB!.claimToken)
  })

  it('returns the number of recovery writes that applied', async () => {
    await backend.enqueue('t', { n: 'a' })
    await backend.enqueue('t', { n: 'b' })
    const jobA = await backend.claimNext<{ n: string }>('t')
    const jobB = await backend.claimNext<{ n: string }>('t')

    await collection.updateMany(
      { _id: { $in: [jobA!.id, jobB!.id] } },
      { $set: { claimedAt: staleClaimedAt } },
    )

    await expect(
      backend.recoverStuckJobs(visibilityTimeoutMs),
    ).resolves.toBe(2)

    // Both jobs are now pending; nothing left to recover.
    await expect(
      backend.recoverStuckJobs(visibilityTimeoutMs),
    ).resolves.toBe(0)
  })
})
