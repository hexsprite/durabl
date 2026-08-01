/**
 * Regression suite for the single-flight guarantee of
 * `claimOrEnqueue` + `dedupeScope: 'pending'` — the call the README documents
 * as a distributed-lock replacement ("never run two at once").
 *
 * Before the fix, nothing enforced it: the pre-read looked only for a *pending*
 * job, the inserted doc was *active*, and `dedupe_pending_idx` partial-filters
 * on `status: 'pending'` — so an active doc fell outside every unique index and
 * N concurrent callers each got a handle and all ran at once.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue dedupe: single-flight coalescing', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  const single = { dedupeKey: 'reschedule:u1', dedupeScope: 'pending' as const }

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('dedupe_jobs'),
    })
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

  it('gives exactly one caller the active slot when several race for the same key', async () => {
    const handles = await Promise.all(
      Array.from({ length: 8 }, () =>
        backend.claimOrEnqueue('reschedule', { u: 1 }, single),
      ),
    )

    expect(handles.filter(Boolean)).toHaveLength(1)
    expect(
      await collection.countDocuments({
        dedupeKey: single.dedupeKey,
        status: 'active',
      }),
    ).toBe(1)
  })

  it('gives exactly one caller the active slot on sequential calls', async () => {
    const first = await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    const second = await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    const third = await backend.claimOrEnqueue('reschedule', { u: 1 }, single)

    expect(first).not.toBeNull()
    expect(second).toBeNull()
    expect(third).toBeNull()
    expect(
      await collection.countDocuments({
        dedupeKey: single.dedupeKey,
        status: 'active',
      }),
    ).toBe(1)
  })

  it('queues exactly one follow-up behind the active run, not one per caller', async () => {
    await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    await backend.claimOrEnqueue('reschedule', { u: 1 }, single)

    // The work that arrived during the active run is not dropped...
    expect(
      await collection.countDocuments({
        dedupeKey: single.dedupeKey,
        status: 'pending',
      }),
    ).toBe(1)
    // ...and a burst collapses into that single follow-up.
    expect(
      await collection.countDocuments({ dedupeKey: single.dedupeKey }),
    ).toBe(2)
  })

  it('lets the next caller win once the active run completes', async () => {
    const first = await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    await first!.complete()

    // Drain the follow-up this caller did not queue (there was none).
    const second = await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    expect(second).not.toBeNull()
    expect(second!.id).not.toBe(first!.id)
  })

  it('does not let a stalled active run block a different dedupe key', async () => {
    const a = await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    const b = await backend.claimOrEnqueue(
      'reschedule',
      { u: 2 },
      { dedupeKey: 'reschedule:u2', dedupeScope: 'pending' },
    )

    expect(a).not.toBeNull()
    expect(b).not.toBeNull()
  })

  it('keeps pending+active scope blocking outright, with nothing queued behind', async () => {
    const both = { dedupeKey: 'welcome:u1' } // default scope

    const first = await backend.claimOrEnqueue('welcome', {}, both)
    const second = await backend.claimOrEnqueue('welcome', {}, both)

    expect(first).not.toBeNull()
    expect(second).toBeNull()
    // 'pending+active' means "no duplicate at all" — no follow-up is queued.
    expect(
      await collection.countDocuments({ dedupeKey: both.dedupeKey }),
    ).toBe(1)
  })

  it('still allows one pending behind one active via plain enqueue', async () => {
    await backend.claimOrEnqueue('reschedule', { u: 1 }, single)
    const queued = await backend.enqueue('reschedule', { u: 1 }, single)
    const queuedTwice = await backend.enqueue('reschedule', { u: 1 }, single)

    expect(queued).not.toBeNull()
    expect(queuedTwice).toBeNull() // capped at one pending
  })
})
