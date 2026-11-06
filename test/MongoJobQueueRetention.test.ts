/**
 * Retention (`cleanupOldJobs`) on MongoJobQueue.
 *
 * Symptom: dead letters (failed/superseded) were deleted with completed jobs on
 * one short clock, and the delete was one unbatched `deleteMany`. The object
 * form gives dead letters their own age (kept by default); deletes run in
 * `batchSize` rounds. The number form must stay exactly as it was, because
 * Focuster calls it that way.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

const DAY = 24 * 60 * 60 * 1000

describe('MongoJobQueue.cleanupOldJobs', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('retention_jobs'),
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

  /** Insert a terminal job whose terminal timestamp is `ageMs` old. */
  async function insertAged(
    id: string,
    status: 'completed' | 'failed' | 'superseded' | 'pending' | 'active',
    ageMs: number,
  ): Promise<void> {
    const at = new Date(Date.now() - ageMs)
    await collection.insertOne({
      _id: id,
      type: 't',
      data: {},
      status,
      priority: 0,
      attempt: 1,
      maxAttempts: 3,
      runAt: at,
      createdAt: at,
      logs: [],
      ...(status === 'completed' ? { completedAt: at } : {}),
      ...(status === 'failed' || status === 'superseded'
        ? { failedAt: at }
        : {}),
    } as JobDoc)
  }

  async function exists(id: string): Promise<boolean> {
    return (await collection.findOne({ _id: id })) !== null
  }

  it('number form deletes completed and failed/superseded jobs older than the age and keeps newer ones', async () => {
    await insertAged('c-old', 'completed', 10 * DAY)
    await insertAged('f-old', 'failed', 10 * DAY)
    await insertAged('s-old', 'superseded', 10 * DAY)
    await insertAged('c-new', 'completed', 1 * DAY)
    await insertAged('f-new', 'failed', 1 * DAY)

    const removed = await backend.cleanupOldJobs(7 * DAY)

    expect(removed).toBe(3)
    expect(await exists('c-old')).toBe(false)
    expect(await exists('f-old')).toBe(false)
    expect(await exists('s-old')).toBe(false)
    expect(await exists('c-new')).toBe(true)
    expect(await exists('f-new')).toBe(true)
  })

  it('no argument keeps the 7 day default for every terminal state', async () => {
    await insertAged('c-old', 'completed', 8 * DAY)
    await insertAged('f-old', 'failed', 8 * DAY)
    await insertAged('c-new', 'completed', 6 * DAY)

    expect(await backend.cleanupOldJobs()).toBe(2)
    expect(await exists('c-new')).toBe(true)
  })

  it('object form without deadLetterMaxAgeMs keeps failed and superseded jobs forever', async () => {
    // Symptom: dead letters were deleted together with completed jobs.
    await insertAged('c-old', 'completed', 400 * DAY)
    await insertAged('f-old', 'failed', 400 * DAY)
    await insertAged('s-old', 'superseded', 400 * DAY)

    const removed = await backend.cleanupOldJobs({ completedMaxAgeMs: DAY })

    expect(removed).toBe(1)
    expect(await exists('c-old')).toBe(false)
    expect(await exists('f-old')).toBe(true)
    expect(await exists('s-old')).toBe(true)
  })

  it('object form with deadLetterMaxAgeMs deletes old dead letters on their own clock', async () => {
    await insertAged('c-mid', 'completed', 10 * DAY)
    await insertAged('f-old', 'failed', 40 * DAY)
    await insertAged('s-old', 'superseded', 40 * DAY)
    await insertAged('f-mid', 'failed', 10 * DAY)

    const removed = await backend.cleanupOldJobs({
      completedMaxAgeMs: 7 * DAY,
      deadLetterMaxAgeMs: 30 * DAY,
    })

    expect(removed).toBe(3)
    expect(await exists('c-mid')).toBe(false)
    expect(await exists('f-old')).toBe(false)
    expect(await exists('s-old')).toBe(false)
    expect(await exists('f-mid')).toBe(true)
  })

  it('object form defaults completed jobs to 7 days', async () => {
    await insertAged('c-old', 'completed', 8 * DAY)
    await insertAged('c-new', 'completed', 6 * DAY)

    expect(await backend.cleanupOldJobs({})).toBe(1)
    expect(await exists('c-new')).toBe(true)
  })

  it('deletes more than batchSize jobs across several batches and returns the total', async () => {
    const ids = Array.from({ length: 25 }, (_, i) => `c-${i}`)
    for (const id of ids) await insertAged(id, 'completed', 10 * DAY)
    await insertAged('c-keep', 'completed', 0)

    const removed = await backend.cleanupOldJobs({
      completedMaxAgeMs: DAY,
      batchSize: 10,
    })

    expect(removed).toBe(25)
    expect(await collection.countDocuments({})).toBe(1)
    expect(await exists('c-keep')).toBe(true)
  })

  it('stops cleanly when the matching count is an exact multiple of batchSize', async () => {
    for (let i = 0; i < 20; i++) await insertAged(`c-${i}`, 'completed', 10 * DAY)

    expect(
      await backend.cleanupOldJobs({ completedMaxAgeMs: DAY, batchSize: 10 }),
    ).toBe(20)
  })

  it('never deletes pending or active jobs', async () => {
    await insertAged('p', 'pending', 400 * DAY)
    await insertAged('a', 'active', 400 * DAY)

    const removed = await backend.cleanupOldJobs({
      completedMaxAgeMs: 0,
      deadLetterMaxAgeMs: 0,
    })

    expect(removed).toBe(0)
    expect(await exists('p')).toBe(true)
    expect(await exists('a')).toBe(true)
  })

  it.each([
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['negative', -1],
  ])('throws on a %s age and deletes nothing', async (_label, bad) => {
    await insertAged('c-old', 'completed', 400 * DAY)
    await insertAged('f-old', 'failed', 400 * DAY)

    await expect(backend.cleanupOldJobs(bad)).rejects.toThrow(/cleanupOldJobs/)
    await expect(
      backend.cleanupOldJobs({ completedMaxAgeMs: bad }),
    ).rejects.toThrow(/completedMaxAgeMs/)
    await expect(
      backend.cleanupOldJobs({ deadLetterMaxAgeMs: bad }),
    ).rejects.toThrow(/deadLetterMaxAgeMs/)

    expect(await collection.countDocuments({})).toBe(2)
  })

  it('throws on a batchSize that is not a positive integer', async () => {
    for (const batchSize of [0, -5, 1.5, Number.NaN]) {
      await expect(backend.cleanupOldJobs({ batchSize })).rejects.toThrow(
        /batchSize/,
      )
    }
  })

  it('is callable through the IJobQueueBackend type after an existence check', async () => {
    await insertAged('c-old', 'completed', 10 * DAY)
    const viaInterface: IJobQueueBackend = backend

    expect(typeof viaInterface.cleanupOldJobs).toBe('function')
    expect(await viaInterface.cleanupOldJobs!(7 * DAY)).toBe(1)
  })
})
