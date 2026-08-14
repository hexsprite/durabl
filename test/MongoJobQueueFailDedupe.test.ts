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
import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest'

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

  it('carries retry backoff and attempt budget to the queued follow-up', async () => {
    const options = {
      ...single,
      backoff: 'fixed' as const,
      backoffDelay: 60_000,
    }
    await backend.claimOrEnqueue('sync', { n: 1 }, options)
    await backend.enqueue('sync-follow-up', { n: 2 }, options)

    const active = await collection.findOne({ status: 'active' })
    const failedAt = Date.now()
    await backend.fail(active!._id, 'transient', active!.claimToken)

    const pending = await collection.findOne({
      dedupeKey: 'u1',
      status: 'pending',
    })
    expect(pending?.attempt).toBe(1)
    expect(pending!.runAt.getTime()).toBeGreaterThanOrEqual(failedAt + 60_000)
    await expect(backend.claimNext('sync')).resolves.toBeNull()

    await collection.updateOne(
      { _id: pending!._id },
      { $set: { runAt: new Date(0) } },
    )
    const claimed =
      await backend.claimNext<{ n: number }>('sync-follow-up')
    expect(claimed?.data.n).toBe(2)
    expect(claimed?.attempt).toBe(2)
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

  it('does not mutate the follower when a lease renews after collision', async () => {
    const handle = await backend.claimOrEnqueue('sync', { n: 1 }, single)
    await backend.enqueue('sync', { n: 2 }, single)
    await collection.updateOne(
      { _id: handle!.id },
      { $set: { claimedAt: new Date(0) } },
    )

    const updateOne = collection.updateOne.bind(collection)
    const renewedAt = new Date()
    let collided = false
    let renewed = false
    vi.spyOn(collection, 'updateOne').mockImplementation(
      async (filter, update, options) => {
        const set = '$set' in update ? update.$set : undefined
        if (collided && !renewed && set?.claimToken) {
          renewed = true
          await updateOne(
            { _id: handle!.id },
            { $set: { claimedAt: renewedAt } },
          )
        }
        try {
          return await updateOne(filter, update, options)
        } catch (error) {
          collided = true
          throw error
        }
      },
    )

    expect(await backend.recoverStuckJobs(0)).toBe(0)
    const job = await collection.findOne({ _id: handle!.id })
    expect(job?.status).toBe('active')
    expect(job?.claimedAt).toEqual(renewedAt)
    const follower = await collection.findOne({
      _id: { $ne: handle!.id },
      status: 'pending',
    })
    expect(follower?.attempt).toBe(0)
  })
})
