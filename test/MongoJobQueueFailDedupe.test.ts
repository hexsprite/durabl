/**
 * Regression suite for the retry path colliding with the pending dedupe index.
 *
 * Found by the property-based harness (du-btm Part A), seed -1076215442.
 *
 * With `dedupeScope: 'pending'` the queue deliberately allows one pending job
 * behind one active job. When the active one FAILS with retries remaining,
 * `fail()` writes `status: 'pending'` — and now there are two pending docs under
 * one key, which `dedupe_pending_idx` forbids. The write throws E11000.
 *
 * The consequence is worse than the throw. `processJob` catches it, logs "error
 * marking job as failed", and moves on — leaving the job `active` forever. The
 * reaper's requeue path performs the same `status: 'pending'` write, so it
 * throws too and can never recover the job either. The result is a permanently
 * active job holding its dedupeKey, which under `pending+active` blocks every
 * future job for that key for good.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue fail(): pending dedupe collision on retry', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  const single = { dedupeKey: 'u1', dedupeScope: 'pending' as const }

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('fail_dedupe_jobs'),
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

  it('does not throw when a retrying job collides with the queued follow-up', async () => {
    const handle = await backend.claimOrEnqueue('sync', { n: 1 }, single)
    expect(handle).not.toBeNull()
    await backend.enqueue('sync', { n: 2 }, single)

    const active = await collection.findOne({ status: 'active' })
    // Before the fix this rejected with E11000 on dedupe_pending_idx.
    await expect(
      backend.fail(active!._id, 'transient', active!.claimToken),
    ).resolves.toBe('applied')
  })

  it('never leaves the job stuck active after a failed retry', async () => {
    const handle = await backend.claimOrEnqueue('sync', { n: 1 }, single)
    await backend.enqueue('sync', { n: 2 }, single)

    const active = await collection.findOne({ status: 'active' })
    await backend.fail(active!._id, 'transient', active!.claimToken).catch(() => {
      /* the bug: swallowed by processJob, job left active */
    })

    // The wedge: an active job holding a dedupeKey that no path can clear.
    const stillActive = await collection.countDocuments({
      _id: active!._id,
      status: 'active',
    })
    expect(stillActive).toBe(0)
    void handle
  })

  it('keeps the queued follow-up runnable', async () => {
    await backend.claimOrEnqueue('sync', { n: 1 }, single)
    await backend.enqueue('sync', { n: 2 }, single)

    const active = await collection.findOne({ status: 'active' })
    await backend.fail(active!._id, 'transient', active!.claimToken)

    // The work still has to happen: exactly one pending job for the key, and it
    // is claimable now that nothing holds the active slot.
    const pending = await collection.countDocuments({
      dedupeKey: 'u1',
      status: 'pending',
    })
    expect(pending).toBe(1)
    const claimed = await backend.claimNext<{ n: number }>('sync')
    expect(claimed).not.toBeNull()
  })

  it('still retries normally when there is no pending duplicate', async () => {
    await backend.enqueue('sync', { n: 1 }, single)
    const claimed = await backend.claimNext('sync')

    await backend.fail(claimed!.id, 'transient', claimed!.claimToken)

    const doc = await collection.findOne({ _id: claimed!.id })
    // Ordinary retry: back to pending, attempt consumed.
    expect(doc?.status).toBe('pending')
    expect(doc?.attempt).toBe(1)
  })

  it('the reaper can recover a stalled job whose key has a pending follow-up', async () => {
    await backend.claimOrEnqueue('sync', { n: 1 }, single)
    await backend.enqueue('sync', { n: 2 }, single)

    // Zero window: every active lease looks expired.
    await expect(backend.recoverStuckJobs(0)).resolves.toBeGreaterThanOrEqual(1)

    const stuck = await collection.countDocuments({ status: 'active' })
    expect(stuck).toBe(0)
  })
})
