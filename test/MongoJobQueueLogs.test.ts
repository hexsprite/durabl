/**
 * Regression: `logs[]` grew without bound and its bytes were billed to the
 * step-journal total.
 *
 * Two consequences, both fixed here. A chatty handler could push the job
 * document to Mongo's hard 16MB cap — and because every terminal write
 * (`fail`, `failFatal`, the reaper's give-up path) appends a log line in the
 * same update, the write that marks the job failed failed too. The job wedged
 * `active`, the reaper re-served it, and the cycle repeated. Separately, log
 * volume counted against the step budget and could trip a `JournalTooLarge` on
 * an otherwise small journal.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue logs[] bounding', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('log_jobs'),
      maxLogEntries: 10,
      maxLogMessageBytes: 100,
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

  it('retains only the newest maxLogEntries', async () => {
    const id = (await backend.enqueue('chatty', {})) as string
    for (let i = 0; i < 50; i++) await backend.log(id, `entry-${i}`)

    const doc = await collection.findOne({ _id: id })
    expect(doc!.logs).toHaveLength(10)
    expect(doc!.logs[0].message).toBe('entry-40')
    expect(doc!.logs[9].message).toBe('entry-49')
  })

  it('clips an over-long message and says so', async () => {
    const id = (await backend.enqueue('chatty', {})) as string
    await backend.log(id, 'x'.repeat(5000))

    const doc = await collection.findOne({ _id: id })
    expect(doc!.logs[0].message.length).toBe(100)
    expect(doc!.logs[0].message).toContain('truncated')
  })

  it('still applies the terminal write after heavy logging', async () => {
    const id = (await backend.enqueue('chatty', {}, { maxAttempts: 1 })) as string
    const claimed = await backend.claimNext('chatty')
    for (let i = 0; i < 200; i++) await backend.log(id, `noise-${i}`)

    const res = await backend.fail(id, 'gave up', claimed!.claimToken)

    expect(res).toEqual({ status: 'failed-terminal' })
    const doc = await collection.findOne({ _id: id })
    expect(doc!.status).toBe('failed')
    expect(doc!.failReason).toBe('gave up')
    // The failure reason survives in the retained tail.
    expect(doc!.logs.at(-1)!.message).toContain('gave up')
  })

  it('keeps the document bounded no matter how chatty the handler is', async () => {
    const id = (await backend.enqueue('chatty', {})) as string
    for (let i = 0; i < 500; i++) await backend.log(id, 'x'.repeat(200))

    const doc = await collection.findOne({ _id: id })
    expect(doc!.logs).toHaveLength(10)
    // 10 retained entries, each clipped to 100 — an upper bound that holds
    // whatever the handler does.
    for (const entry of doc!.logs) expect(entry.message.length).toBeLessThanOrEqual(100)
  })

  it('does not bill log volume to the step-journal budget', async () => {
    const id = (await backend.enqueue('chatty', {})) as string
    for (let i = 0; i < 100; i++) await backend.log(id, 'x'.repeat(90))

    const doc = await collection.findOne({ _id: id })
    expect(doc!.journalBytes ?? 0).toBe(0)
  })
})
