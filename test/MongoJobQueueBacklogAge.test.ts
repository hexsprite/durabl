/**
 * Backlog age at the Mongo layer (du-8wi).
 *
 * Depth cannot separate a healthy queue from a stuck one — 5 jobs waiting 40
 * minutes is an incident, 5000 draining fast is fine. These tests pin the two
 * properties that make the metric trustworthy (future-dated jobs excluded, only
 * pending counted) plus the query plan, since a metric that collection-scans on
 * a busy queue is one nobody can afford to poll.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue getStats: backlog age', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('backlog_jobs'),
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

  it('reports lag for an overdue pending job', async () => {
    await backend.enqueue('sync', { n: 1 })
    await collection.updateOne(
      { type: 'sync' },
      { $set: { runAt: new Date(Date.now() - 90_000) } },
    )

    const stats = await backend.getStats('sync')
    expect(stats.oldestPendingLagMs).toBeGreaterThanOrEqual(90_000)
    expect(stats.oldestPendingRunAt).toBeInstanceOf(Date)
  })

  it('counts a delayed job as pending but not as backlog', async () => {
    await backend.enqueue('sync', { n: 1 }, { delay: 3_600_000 })

    const stats = await backend.getStats('sync')
    expect(stats.pending).toBe(1)
    expect(stats.oldestPendingRunAt).toBeNull()
    expect(stats.oldestPendingLagMs).toBe(0)
  })

  it('does not count active jobs as backlog', async () => {
    await backend.claimOrEnqueue('sync', { n: 1 })

    const stats = await backend.getStats('sync')
    expect(stats.active).toBe(1)
    expect(stats.oldestPendingRunAt).toBeNull()
  })

  it('reports zero on an empty queue', async () => {
    const stats = await backend.getStats('sync')
    expect(stats.oldestPendingRunAt).toBeNull()
    expect(stats.oldestPendingLagMs).toBe(0)
  })

  it('scopes to the requested type', async () => {
    await backend.enqueue('other', { n: 1 })
    const stats = await backend.getStats('sync')
    expect(stats.oldestPendingRunAt).toBeNull()
  })

  it('serves the backlog query from an index, not a collection scan', async () => {
    for (let i = 0; i < 25; i++) await backend.enqueue('sync', { i })

    const plan = await collection
      .find({ type: 'sync', status: 'pending', runAt: { $lte: new Date() } })
      .sort({ runAt: 1 })
      .limit(1)
      .explain('queryPlanner')

    const winning = JSON.stringify(
      (plan as { queryPlanner: { winningPlan: unknown } }).queryPlanner
        .winningPlan,
    )
    expect(winning).toContain('IXSCAN')
    expect(winning).not.toContain('COLLSCAN')
  })
})
