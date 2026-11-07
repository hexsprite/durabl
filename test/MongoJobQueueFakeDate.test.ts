/**
 * Default `MongoJobQueue` follows the process `Date`, so fake timers work.
 *
 * Symptom (du-u5f): 0.6.0 stamped every lease and schedule time from the Mongo
 * server clock. Focuster's server tests freeze `Date` at 1977 with sinon fake
 * timers. The queue measured a constant 49-year offset, so its "now" never
 * advanced. Times that test code wrote with local `Date` (`runAt = Date.now() +
 * 60000`) landed decades in the past against the queue's stamps. A timed-out
 * orchestrated job was reclaimed at once and the run ended in a LeaseLostError
 * cascade. The server clock is now opt-in (`clock: 'server'`).
 *
 * Only `Date` is faked. The Mongo driver and the test runner need real timers.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'
import { silentLogger } from './testLogger'

const FROZEN = new Date('1977-03-21T09:00:00Z')
const isHello = (args: unknown[]): boolean =>
  typeof args[0] === 'object' && args[0] !== null && 'hello' in args[0]

describe('MongoJobQueue with a faked Date', () => {
  let db: Db
  let collectionName: string
  let backend: MongoJobQueue | null = null
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    collectionName = uniqueCollectionName('fake_date_jobs')
    collection = db.collection<JobDoc>(collectionName)
  })

  afterEach(async () => {
    vi.useRealTimers()
    vi.restoreAllMocks()
    await backend?.shutdown()
    backend = null
    await collection.drop().catch(() => {
      /* already gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it('follows the faked Date for stamps and schedules (default config)', async () => {
    vi.useFakeTimers({ toFake: ['Date'], now: FROZEN })
    backend = new MongoJobQueue({ db, collectionName, logger: silentLogger })
    await backend.startup()

    const id = await backend.enqueue('t', {}, { maxAttempts: 3 })
    const job = await backend.claimNext('t')
    expect(job).not.toBeNull()
    const claimed = await collection.findOne({ _id: id! })
    expect(claimed!.claimedAt!.getUTCFullYear()).toBe(1977)

    await backend.fail(job!.id, 'transient', job!.claimToken)
    const failed = await collection.findOne({ _id: id! })
    expect(failed!.status).toBe('pending')
    expect(failed!.runAt.getUTCFullYear()).toBe(1977)

    // Consumer code schedules with local (faked) Date.
    const runAt = new Date(Date.now() + 60_000)
    await collection.updateOne({ _id: id! }, { $set: { runAt } })
    expect(await backend.claimNext('t')).toBeNull()

    vi.setSystemTime(new Date(runAt.getTime() + 1))
    const again = await backend.claimNext('t')
    expect(again?.id).toBe(id)
  })

  it('never sends hello in the default mode', async () => {
    const spy = vi.spyOn(db, 'command')
    backend = new MongoJobQueue({ db, collectionName, logger: silentLogger })
    await backend.startup()
    await backend.enqueue('t', {})
    expect(spy.mock.calls.filter(isHello)).toHaveLength(0)
  })

  it("measures the server clock with hello when clock is 'server'", async () => {
    const spy = vi.spyOn(db, 'command')
    backend = new MongoJobQueue({
      db,
      collectionName,
      logger: silentLogger,
      clock: 'server',
    })
    await backend.startup()
    expect(spy.mock.calls.filter(isHello)).toHaveLength(1)
  })
})
