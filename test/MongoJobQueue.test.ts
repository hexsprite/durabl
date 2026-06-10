import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { Collection, Db } from 'mongodb'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    const collectionName = uniqueCollectionName('test_jobs')
    backend = new MongoJobQueue({ db, collectionName })
    collection = backend.getCollection()
    await backend.startup()
  })

  afterEach(async () => {
    await backend.shutdown()
    await collection.drop().catch(() => {
      /* ignore */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  describe('enqueue()', () => {
    it('creates a pending job', async () => {
      const jobId = await backend.enqueue('testJob', { foo: 'bar' })

      expect(jobId).toEqual(expect.any(String))
      const job = await collection.findOne({ _id: jobId! })
      expect(job).toBeDefined()
      expect(job!.type).toBe('testJob')
      expect(job!.data).toEqual({ foo: 'bar' })
      expect(job!.status).toBe('pending')
      expect(job!.attempt).toBe(0)
      expect(job!.maxAttempts).toBe(3)
    })

    it('respects options', async () => {
      const jobId = await backend.enqueue(
        'job',
        {},
        { priority: 5, maxAttempts: 10 },
      )

      const job = await collection.findOne({ _id: jobId! })
      expect(job!.priority).toBe(5)
      expect(job!.maxAttempts).toBe(10)
    })

    it('applies delay to runAt', async () => {
      const before = Date.now()
      const jobId = await backend.enqueue('job', {}, { delay: 5000 })

      const job = await collection.findOne({ _id: jobId! })
      expect(job!.runAt.getTime()).toBeGreaterThanOrEqual(before + 5000)
    })

    it('returns null for duplicate dedupeKey', async () => {
      const id1 = await backend.enqueue('job', {}, { dedupeKey: 'unique' })
      const id2 = await backend.enqueue('job', {}, { dedupeKey: 'unique' })

      expect(id1).toEqual(expect.any(String))
      expect(id2).toBeNull()
    })

    it('allows new job after previous completes', async () => {
      const id1 = await backend.enqueue('job', {}, { dedupeKey: 'unique' })
      await backend.complete(id1!)

      const id2 = await backend.enqueue('job', {}, { dedupeKey: 'unique' })
      expect(id2).toEqual(expect.any(String))
      expect(id2).not.toBe(id1)
    })
  })

  describe('claimOrEnqueue()', () => {
    it('creates and claims job atomically', async () => {
      const handle = await backend.claimOrEnqueue('job', { x: 1 })

      expect(handle).toBeDefined()
      expect(handle!.data).toEqual({ x: 1 })

      const job = await collection.findOne({ _id: handle!.id })
      expect(job!.status).toBe('active')
      expect(job!.attempt).toBe(1)
    })

    it('returns null when pending job exists', async () => {
      await backend.enqueue('job', {}, { dedupeKey: 'user:123' })
      const handle = await backend.claimOrEnqueue(
        'job',
        {},
        { dedupeKey: 'user:123' },
      )

      expect(handle).toBeNull()
    })

    it('handle.complete() marks job completed', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      await handle!.complete()

      const job = await collection.findOne({ _id: handle!.id })
      expect(job!.status).toBe('completed')
    })

    it('handle.fail() marks job failed when exhausted', async () => {
      const handle = await backend.claimOrEnqueue('job', {}, { maxAttempts: 1 })
      await handle!.fail('error')

      const job = await collection.findOne({ _id: handle!.id })
      expect(job!.status).toBe('failed')
    })
  })

  describe('claimNext()', () => {
    it('claims the next pending job', async () => {
      await backend.enqueue('job', { n: 1 })
      await backend.enqueue('job', { n: 2 })

      const job = await backend.claimNext('job')

      expect(job).toBeDefined()
      expect(job!.data).toEqual({ n: 1 })
      expect(job!.status).toBe('active')
      expect(job!.attempt).toBe(1)
    })

    it('returns null when no pending jobs', async () => {
      const job = await backend.claimNext('job')
      expect(job).toBeNull()
    })

    it('respects priority ordering', async () => {
      await backend.enqueue('job', { n: 'low' }, { priority: 10 })
      await backend.enqueue('job', { n: 'high' }, { priority: 1 })

      const job = await backend.claimNext('job')
      expect(job!.data).toEqual({ n: 'high' })
    })

    it('skips delayed jobs', async () => {
      await backend.enqueue('job', { n: 'delayed' }, { delay: 60000 })
      await backend.enqueue('job', { n: 'ready' })

      const job = await backend.claimNext('job')
      expect(job!.data).toEqual({ n: 'ready' })
    })
  })

  describe('fail()', () => {
    it('returns job to pending if attempts remain', async () => {
      const jobId = await backend.enqueue('job', {}, { maxAttempts: 3 })
      const job = await backend.claimNext('job')
      await backend.fail(job!.id, 'error')

      const updated = await collection.findOne({ _id: jobId! })
      expect(updated!.status).toBe('pending')
      expect(updated!.attempt).toBe(1)
    })

    it('marks job failed when max attempts reached', async () => {
      const jobId = await backend.enqueue('job', {}, { maxAttempts: 1 })
      const job = await backend.claimNext('job')
      await backend.fail(job!.id, 'error')

      const updated = await collection.findOne({ _id: jobId! })
      expect(updated!.status).toBe('failed')
      expect(updated!.failReason).toBe('error')
    })

    // Regression (du-0so): the retry branch left runAt unchanged, so a failed
    // job was instantly re-claimable. A fast-failing handler then burned all
    // attempts in milliseconds and a downstream outage became an instant-retry
    // storm. Retry must push runAt into the future (backoff).
    it('delays the retry — runAt is in the future, not immediately claimable', async () => {
      const jobId = await backend.enqueue('job', {}, { maxAttempts: 3 })
      const job = await backend.claimNext('job')

      const before = Date.now()
      await backend.fail(job!.id, 'transient')

      const updated = await collection.findOne({ _id: jobId! })
      expect(updated!.status).toBe('pending')
      expect(updated!.runAt.getTime()).toBeGreaterThan(before)
      // claimNext only takes runAt <= now, so it stays unclaimed.
      expect(await backend.claimNext('job')).toBeNull()
    })

    it('honors a fixed backoff delay on retry', async () => {
      const jobId = await backend.enqueue(
        'job',
        {},
        { maxAttempts: 3, backoff: 'fixed', backoffDelay: 5000 },
      )
      const job = await backend.claimNext('job')

      const before = Date.now()
      await backend.fail(job!.id, 'transient')

      const updated = await collection.findOne({ _id: jobId! })
      const delayMs = updated!.runAt.getTime() - before
      // Fixed 5s, allow scheduling slack.
      expect(delayMs).toBeGreaterThanOrEqual(4900)
      expect(delayMs).toBeLessThanOrEqual(6000)
    })
  })

  describe('failFatal()', () => {
    it('marks job failed immediately', async () => {
      const jobId = await backend.enqueue('job', {}, { maxAttempts: 10 })
      const job = await backend.claimNext('job')
      await backend.failFatal(job!.id, 'fatal error')

      const updated = await collection.findOne({ _id: jobId! })
      expect(updated!.status).toBe('failed')
      expect(updated!.failReason).toBe('fatal error')
    })
  })

  describe('getStats()', () => {
    it('returns correct counts', async () => {
      await backend.enqueue('job', {})
      await backend.enqueue('job', {})
      await backend.claimOrEnqueue('job', {})
      const claimed = await backend.claimNext('job')
      await backend.complete(claimed!.id)

      const stats = await backend.getStats()

      expect(stats.pending).toBe(1)
      expect(stats.active).toBe(1)
      expect(stats.completed).toBe(1)
      expect(stats.failed).toBe(0)
    })

    it('filters by type', async () => {
      await backend.enqueue('typeA', {})
      await backend.enqueue('typeB', {})

      const statsA = await backend.getStats('typeA')
      expect(statsA.pending).toBe(1)
    })
  })

  describe('recoverStuckJobs()', () => {
    it('recovers jobs past visibility timeout', async () => {
      await backend.enqueue('job', {})
      const job = await backend.claimNext('job')

      // Backdate claimedAt to simulate a stuck job.
      await collection.updateOne(
        { _id: job!.id },
        { $set: { claimedAt: new Date(Date.now() - 400000) } },
      )

      const recovered = await backend.recoverStuckJobs(300000)

      expect(recovered).toBe(1)
      const updated = await collection.findOne({ _id: job!.id })
      expect(updated!.status).toBe('pending')
    })

    // Regression (du-5ws): a handler that wedges the worker stalls, gets
    // recovered, re-claimed, wedges again... If recovery re-queues with no
    // backoff, the job is immediately claimable again -> hot retry loop that
    // pegs CPU/Mongo every visibility window. Recovery must push runAt into
    // the future so the job cannot be re-claimed instantly.
    it('schedules recovered job in the future (backoff), not immediately claimable', async () => {
      await backend.enqueue('job', {}, { maxAttempts: 5 })
      const job = await backend.claimNext('job')
      await collection.updateOne(
        { _id: job!.id },
        { $set: { claimedAt: new Date(Date.now() - 400000) } },
      )

      const before = Date.now()
      await backend.recoverStuckJobs(300000)

      const updated = await collection.findOne({ _id: job!.id })
      expect(updated!.status).toBe('pending')
      // runAt pushed past now: not immediately re-claimable.
      expect(updated!.runAt.getTime()).toBeGreaterThan(before)
      // And claimNext (which only takes runAt <= now) returns nothing yet.
      const reclaimed = await backend.claimNext('job')
      expect(reclaimed).toBeNull()
    })

    // Regression (du-5ws): the original updateMany blindly set every stuck
    // active job back to 'pending' with no maxAttempts check. A job that has
    // exhausted its retries but stalls (e.g. wedged on the final attempt) was
    // resurrected forever instead of being failed. Past the cap it must go
    // terminal.
    it('fails a stalled job that has exhausted its attempts instead of re-queuing', async () => {
      await backend.enqueue('job', {}, { maxAttempts: 2 })
      const job = await backend.claimNext('job') // attempt -> 1
      // Simulate it already being on its final attempt and then stalling.
      await collection.updateOne(
        { _id: job!.id },
        {
          $set: {
            attempt: 2,
            claimedAt: new Date(Date.now() - 400000),
          },
        },
      )

      const handled = await backend.recoverStuckJobs(300000)

      expect(handled).toBe(1)
      const updated = await collection.findOne({ _id: job!.id })
      expect(updated!.status).toBe('failed')
      expect(updated!.failedAt).toBeInstanceOf(Date)
      expect(updated!.failReason).toMatch(/stall/i)
    })

    it('does not increment attempt unboundedly on recovery', async () => {
      // Recovery itself must not bump attempt — the re-claim does that.
      await backend.enqueue('job', {}, { maxAttempts: 5 })
      const job = await backend.claimNext('job') // attempt -> 1
      await collection.updateOne(
        { _id: job!.id },
        { $set: { claimedAt: new Date(Date.now() - 400000) } },
      )

      await backend.recoverStuckJobs(300000)

      const updated = await collection.findOne({ _id: job!.id })
      expect(updated!.attempt).toBe(1)
    })
  })

  describe('batchHeartbeat()', () => {
    // du-g3t: keep N running jobs alive with ONE updateMany instead of N
    // updateOne calls, so heartbeat write load is independent of concurrency.
    it('extends claimedAt for all given running jobs in a single write', async () => {
      const ids: string[] = []
      for (let i = 0; i < 3; i++) {
        await backend.enqueue('job', { i })
        const job = await backend.claimNext<{ i: number }>('job')
        ids.push(job!.id)
      }

      // Backdate all claimedAt timestamps.
      const stale = new Date(Date.now() - 200000)
      await collection.updateMany(
        { _id: { $in: ids } },
        { $set: { claimedAt: stale } },
      )

      const before = Date.now()
      await backend.batchHeartbeat(ids)

      for (const id of ids) {
        const doc = await collection.findOne({ _id: id })
        expect(doc!.claimedAt!.getTime()).toBeGreaterThanOrEqual(before)
      }
    })

    it('only touches active jobs, never completed/failed ones', async () => {
      await backend.enqueue('job', {})
      const active = await backend.claimNext('job')
      await backend.enqueue('job2', {})
      const done = await backend.claimNext('job2')
      await backend.complete(done!.id)
      const completedAt = (await collection.findOne({ _id: done!.id }))!
        .completedAt

      await backend.batchHeartbeat([active!.id, done!.id])

      const doneDoc = await collection.findOne({ _id: done!.id })
      // Completed job untouched: still completed, claimedAt not refreshed past
      // completion.
      expect(doneDoc!.status).toBe('completed')
      expect(doneDoc!.completedAt).toEqual(completedAt)
    })

    it('is a no-op for an empty id list', async () => {
      await expect(backend.batchHeartbeat([])).resolves.toBeUndefined()
    })
  })

  describe('dedupeScope behavior', () => {
    it('pending+active blocks both pending and active', async () => {
      const handle = await backend.claimOrEnqueue(
        'job',
        {},
        { dedupeKey: 'key', dedupeScope: 'pending+active' },
      )
      expect(handle).toBeDefined()

      const id2 = await backend.enqueue(
        'job',
        {},
        { dedupeKey: 'key', dedupeScope: 'pending+active' },
      )
      expect(id2).toBeNull()

      await handle!.complete()

      const id3 = await backend.enqueue(
        'job',
        {},
        { dedupeKey: 'key', dedupeScope: 'pending+active' },
      )
      expect(id3).toEqual(expect.any(String))
    })

    it('pending scope allows 1 pending + 1 active', async () => {
      const handle = await backend.claimOrEnqueue(
        'job',
        { n: 1 },
        { dedupeKey: 'key', dedupeScope: 'pending' },
      )
      expect(handle).toBeDefined()

      const id2 = await backend.enqueue(
        'job',
        { n: 2 },
        { dedupeKey: 'key', dedupeScope: 'pending' },
      )
      expect(id2).toEqual(expect.any(String))

      const id3 = await backend.enqueue(
        'job',
        { n: 3 },
        { dedupeKey: 'key', dedupeScope: 'pending' },
      )
      expect(id3).toBeNull()
    })
  })
})
