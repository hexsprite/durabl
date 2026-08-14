/**
 * Regression suite for du-pz9: a dedupe-blocked candidate must be SKIPPED, not
 * surfaced as a thrown driver error.
 *
 * Symptom: with job A active and job B pending under one `dedupeScope: 'pending'`
 * key, `claimNext` tried to flip B to `active`, violated `dedupe_active_idx`, and
 * let the E11000 escape. `JobQueue.claimAndProcess` catches any claim throw and
 * applies exponential backoff (1s doubling to 60s) to the ProcessorState — which
 * is keyed on job TYPE, not on dedupeKey. So one contended key stalled the poll
 * loop for every other key of that type, escalating, while logging errors on a
 * queue that was working exactly as designed.
 *
 * The mutual exclusion itself was never broken and must stay unbroken: these
 * tests assert both that contention is invisible to the caller AND that no key
 * ever has two active jobs.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import { CLAIM_INDEX_NAME } from '../src/backends/mongoJobIndexes'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue claimNext: dedupe-blocked candidates', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  const single = (key: string) => ({
    dedupeKey: key,
    dedupeScope: 'pending' as const,
  })

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('dedupe_claim_jobs'),
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

  it('does not throw when the only candidate is blocked by a live key', async () => {
    const handle = await backend.claimOrEnqueue('sync', { n: 1 }, single('u1'))
    expect(handle).not.toBeNull()
    await backend.enqueue('sync', { n: 2 }, single('u1'))

    // Before the fix this rejected with E11000 from dedupe_active_idx.
    await expect(backend.claimNext('sync')).resolves.toBeNull()
  })

  it('claims a different key instead of stalling on the blocked one', async () => {
    // u1 is contended: one active, one pending behind it.
    await backend.claimOrEnqueue('sync', { user: 'u1' }, single('u1'))
    await backend.enqueue('sync', { user: 'u1' }, single('u1'))
    // u2 is free and claimable. It must not be starved by u1's contention.
    await backend.enqueue('sync', { user: 'u2' }, single('u2'))

    const claimed = await backend.claimNext<{ user: string }>('sync')
    expect(claimed?.data.user).toBe('u2')
  })

  it('advances past several blocked keys to reach a claimable job', async () => {
    for (const key of ['u1', 'u2', 'u3']) {
      await backend.claimOrEnqueue('sync', { user: key }, single(key))
      await backend.enqueue('sync', { user: key }, single(key))
    }
    await backend.enqueue('sync', { user: 'free' }, single('free'))

    const claimed = await backend.claimNext<{ user: string }>('sync')
    expect(claimed?.data.user).toBe('free')
  })

  it('reaches a claimable job behind more than twenty blocked keys', async () => {
    for (let i = 0; i < 21; i++) {
      const key = `blocked-${i}`
      await backend.claimOrEnqueue('sync', { user: key }, single(key))
      await backend.enqueue('sync', { user: key }, single(key))
    }
    await backend.enqueue('sync', { user: 'free' }, single('free'))

    const claimed = await backend.claimNext<{ user: string }>('sync')
    expect(claimed?.data.user).toBe('free')
  })

  it('does not block the same key in an independent dedupe scope', async () => {
    await backend.claimOrEnqueue('sync', { scope: 'pending-owner' }, {
      dedupeKey: 'shared',
      dedupeScope: 'pending',
    })
    await backend.enqueue('sync', { scope: 'pending-follower' }, {
      dedupeKey: 'shared',
      dedupeScope: 'pending',
    })
    await backend.enqueue('sync', { scope: 'pending+active' }, {
      dedupeKey: 'shared',
      dedupeScope: 'pending+active',
    })

    const claimed = await backend.claimNext<{ scope: string }>('sync')
    expect(claimed?.data.scope).toBe('pending+active')
  })

  it('never lets a dedupeKey hold two active jobs', async () => {
    await backend.claimOrEnqueue('sync', { n: 1 }, single('u1'))
    await backend.enqueue('sync', { n: 2 }, single('u1'))

    // Hammer the claim path; the invariant must hold on every attempt.
    await Promise.all(
      Array.from({ length: 8 }, () => backend.claimNext('sync')),
    )

    const active = await collection.countDocuments({
      dedupeKey: 'u1',
      status: 'active',
    })
    expect(active).toBe(1)
  })

  it('leaves the blocked job claimable once the holder finishes', async () => {
    const handle = await backend.claimOrEnqueue('sync', { n: 1 }, single('u1'))
    await backend.enqueue('sync', { n: 2 }, single('u1'))

    expect(await backend.claimNext('sync')).toBeNull()

    await handle!.complete()

    const claimed = await backend.claimNext<{ n: number }>('sync')
    expect(claimed?.data.n).toBe(2)
  })

  it('still returns null when there is genuinely nothing to claim', async () => {
    expect(await backend.claimNext('sync')).toBeNull()
  })

  it('keeps using the claim index and adds no blocking sort', async () => {
    await backend.claimOrEnqueue('sync', { n: 1 }, single('u1'))
    await backend.enqueue('sync', { n: 2 }, single('u1'))
    for (let i = 0; i < 20; i++) {
      await backend.enqueue('sync', { i }, single(`free-${i}`))
    }

    const plan = await collection
      .find({
        type: 'sync',
        status: 'pending',
        runAt: { $lte: new Date() },
        $nor: [{ dedupeKey: 'u1', dedupeScope: 'pending' }],
      })
      .sort({ priority: 1, runAt: 1 })
      .explain('queryPlanner')

    // Scope to the WINNING plan: rejectedPlans legitimately contain SORT
    // stages (they are the alternatives the planner discarded).
    const winning = JSON.stringify(
      (plan as { queryPlanner: { winningPlan: unknown } }).queryPlanner
        .winningPlan,
    )
    expect(winning).toContain(CLAIM_INDEX_NAME)
    expect(winning).not.toContain('"stage":"SORT"')
  })
})
