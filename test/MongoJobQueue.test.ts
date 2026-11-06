import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { Collection, Db } from 'mongodb'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type {
  CompleteJobResult,
  FailFatalJobResult,
  FailJobResult,
  JobDoc,
} from '../src/types'

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

    it('handle.complete() returns the completed result', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      const result: CompleteJobResult = await handle!.complete()

      expect(result).toEqual({ status: 'completed' })
      const job = await collection.findOne({ _id: handle!.id })
      expect(job!.status).toBe('completed')
    })

    it('handle.fail() returns the terminal failure result', async () => {
      const handle = await backend.claimOrEnqueue('job', {}, { maxAttempts: 1 })
      const result: FailJobResult = await handle!.fail('error')

      expect(result).toEqual({ status: 'failed-terminal' })
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

  // Symptom: every rolling deploy during a long job used up one retry.
  // `release()` kept the attempt that `claimNext` had counted, so three
  // deploys left a maxAttempts=3 job with a single real try.
  describe('release() attempt budget', () => {
    it('release returns the claimed attempt so shutdown does not consume a retry', async () => {
      const id = await backend.enqueue('job', {}, { maxAttempts: 3 })
      const claimed = await backend.claimNext('job')
      expect(claimed!.attempt).toBe(1)

      const result = await backend.release(id!, claimed!.claimToken)

      expect(result).toEqual({ status: 'released' })
      const stored = await collection.findOne({ _id: id! })
      expect(stored!.status).toBe('pending')
      expect(stored!.attempt).toBe(0)
    })

    it('survives as many releases as maxAttempts without exhausting the job', async () => {
      const id = await backend.enqueue('job', {}, { maxAttempts: 3 })

      for (let deploy = 0; deploy < 4; deploy++) {
        const claimed = await backend.claimNext('job')
        expect(claimed).not.toBeNull()
        expect(claimed!.attempt).toBe(1)
        await backend.release(id!, claimed!.claimToken)
      }

      const stored = await collection.findOne({ _id: id! })
      expect(stored).toMatchObject({ status: 'pending', attempt: 0 })
    })

    it('keeps a real failure attempt: only the released claim is given back', async () => {
      const id = await backend.enqueue('job', {}, { maxAttempts: 3 })
      const first = await backend.claimNext('job')
      await backend.fail(id!, 'boom', first!.claimToken)
      await collection.updateOne({ _id: id! }, { $set: { runAt: new Date() } })

      const second = await backend.claimNext('job')
      expect(second!.attempt).toBe(2)
      await backend.release(id!, second!.claimToken)

      expect((await collection.findOne({ _id: id! }))!.attempt).toBe(1)
    })

    it('never takes the stored attempt below zero', async () => {
      const id = await backend.enqueue('job', {})
      const claimed = await backend.claimNext('job')
      // A manual re-activation can leave an active job at attempt 0.
      await collection.updateOne({ _id: id! }, { $set: { attempt: 0 } })

      await backend.release(id!, claimed!.claimToken)

      expect((await collection.findOne({ _id: id! }))!.attempt).toBe(0)
    })

    it('a stale release does not touch the replacement claim attempt', async () => {
      const id = await backend.enqueue('job', {})
      const first = await backend.claimNext('job')
      await backend.release(id!, first!.claimToken)
      await backend.claimNext('job')

      const stale = await backend.release(id!, first!.claimToken)

      expect(stale).toEqual({ status: 'lease-lost' })
      expect((await collection.findOne({ _id: id! }))!.attempt).toBe(1)
    })
  })

  // Symptom: a pending job with attempt >= maxAttempts (manual edit, legacy
  // document) was handed to a handler beyond its retry budget.
  describe('claim attempt budget', () => {
    async function makeExhausted(
      id: string,
      attempt = 3,
    ): Promise<void> {
      await collection.updateOne({ _id: id }, { $set: { attempt } })
    }

    it('claimNext never hands out a pending job whose attempts are spent', async () => {
      const id = await backend.enqueue('job', {}, { maxAttempts: 3 })
      await makeExhausted(id!)

      expect(await backend.claimNext('job')).toBeNull()

      expect(await collection.findOne({ _id: id! })).toMatchObject({
        status: 'failed',
        failureKind: 'retries-exhausted',
        attempt: 3,
      })
    })

    it('claimNext fails the spent job and still claims the next due job', async () => {
      const spent = await backend.enqueue(
        'job',
        { n: 'spent' },
        { maxAttempts: 2, priority: 0 },
      )
      await backend.enqueue('job', { n: 'fresh' }, { priority: 5 })
      await makeExhausted(spent!, 2)

      const claimed = await backend.claimNext<{ n: string }>('job')

      expect(claimed!.data).toEqual({ n: 'fresh' })
      expect(claimed!.attempt).toBe(1)
      expect((await collection.findOne({ _id: spent! }))!.status).toBe('failed')
    })

    it('a spent pending job does not hold its dedupe slot', async () => {
      const id = await backend.enqueue(
        'job',
        {},
        { maxAttempts: 3, dedupeKey: 'k' },
      )
      await makeExhausted(id!)
      expect(await backend.enqueue('job', {}, { dedupeKey: 'k' })).toBeNull()

      await backend.claimNext('job')

      expect(
        await backend.enqueue('job', {}, { dedupeKey: 'k' }),
      ).not.toBeNull()
    })

    it('claimNextByKey never hands out a spent job either', async () => {
      const id = await backend.enqueue(
        'job',
        {},
        { maxAttempts: 3, dedupeKey: 'k', dedupeScope: 'pending' },
      )
      await makeExhausted(id!)

      expect(await backend.claimNextByKey('job', 'k')).toBeNull()

      expect(await collection.findOne({ _id: id! })).toMatchObject({
        status: 'failed',
        failureKind: 'retries-exhausted',
      })
    })

    it('records why the spent job failed in its log', async () => {
      const id = await backend.enqueue('job', {}, { maxAttempts: 1 })
      await makeExhausted(id!, 1)

      await backend.claimNext('job')

      const stored = await collection.findOne({ _id: id! })
      expect(stored!.failedAt).toBeInstanceOf(Date)
      expect(stored!.failReason).toMatch(/exhausted/i)
      expect(stored!.logs?.at(-1)?.message).toMatch(/exhausted/i)
    })
  })

  describe('findOne()', () => {
    it('does not expose the live fencing claimToken (claim path only)', async () => {
      // The claim path must see the token (processJob fences writes with it)…
      const jobId = await backend.enqueue('job', {})
      const claimed = await backend.claimNext('job')
      expect(claimed!.claimToken).toEqual(expect.any(String))

      // …but the general read view must not leak it: any findOne caller could
      // otherwise forge lease-fenced writes against the live owner's job.
      const found = await backend.findOne({ _id: jobId })
      expect(found).not.toBeNull()
      expect(found!.claimToken).toBeUndefined()
    })
  })

  describe('fail()', () => {
    it('returns job to pending if attempts remain', async () => {
      const jobId = await backend.enqueue('job', {}, { maxAttempts: 3 })
      const job = await backend.claimNext('job')
      const result: FailJobResult = await backend.fail(job!.id, 'error')

      expect(result).toEqual({ status: 'retry-scheduled' })
      const updated = await collection.findOne({ _id: jobId! })
      expect(updated!.status).toBe('pending')
      expect(updated!.attempt).toBe(1)
    })

    it('marks job failed when max attempts are reached', async () => {
      const jobId = await backend.enqueue('job', {}, { maxAttempts: 1 })
      const job = await backend.claimNext('job')
      const result: FailJobResult = await backend.fail(job!.id, 'error')

      expect(result).toEqual({ status: 'failed-terminal' })
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
      const result: FailFatalJobResult = await backend.failFatal(
        job!.id,
        'fatal error',
      )

      expect(result).toEqual({ status: 'failed-terminal' })
      const updated = await collection.findOne({ _id: jobId! })
      expect(updated!.status).toBe('failed')
      expect(updated!.failReason).toBe('fatal error')
    })
  })

  describe('terminal transition results and fencing', () => {
    async function claimThenReclaim(): Promise<{
      id: string
      staleToken: string
    }> {
      await backend.enqueue('fenced', {})
      const claimed = await backend.claimNext('fenced')
      await collection.updateOne(
        { _id: claimed!.id },
        { $set: { claimToken: 'new-owner-token' } },
      )
      return { id: claimed!.id, staleToken: claimed!.claimToken! }
    }

    it('reports the stored completed status to later failure operations', async () => {
      await backend.enqueue('job', {})
      const job = await backend.claimNext('job')
      await backend.complete(job!.id, job!.claimToken)

      expect(await backend.fail(job!.id, 'late', job!.claimToken)).toEqual({
        status: 'already-terminal',
        terminalStatus: 'completed',
      })
      expect(
        await backend.failFatal(job!.id, 'late', job!.claimToken),
      ).toEqual({
        status: 'already-terminal',
        terminalStatus: 'completed',
      })
    })

    it('reports the stored failed status to a later completion', async () => {
      await backend.enqueue('job', {})
      const job = await backend.claimNext('job')
      await backend.failFatal(job!.id, 'fatal', job!.claimToken)

      expect(await backend.complete(job!.id, job!.claimToken)).toEqual({
        status: 'already-terminal',
        terminalStatus: 'failed',
      })
    })

    it('fences complete() with a stale claim token', async () => {
      const { id, staleToken } = await claimThenReclaim()

      expect(await backend.complete(id, staleToken)).toEqual({
        status: 'lease-lost',
      })
      expect((await collection.findOne({ _id: id }))!.status).toBe('active')
    })

    it('fences fail() with a stale claim token', async () => {
      const { id, staleToken } = await claimThenReclaim()

      expect(await backend.fail(id, 'stale', staleToken)).toEqual({
        status: 'lease-lost',
      })
      expect((await collection.findOne({ _id: id }))!.status).toBe('active')
    })

    it('fences failFatal() with a stale claim token', async () => {
      const { id, staleToken } = await claimThenReclaim()

      expect(await backend.failFatal(id, 'stale', staleToken)).toEqual({
        status: 'lease-lost',
      })
      expect((await collection.findOne({ _id: id }))!.status).toBe('active')
    })

    it('reports missing jobs for every terminal operation', async () => {
      expect(await backend.complete('missing', 'token')).toEqual({
        status: 'not-found',
      })
      expect(await backend.fail('missing', 'error', 'token')).toEqual({
        status: 'not-found',
      })
      expect(await backend.failFatal('missing', 'fatal', 'token')).toEqual({
        status: 'not-found',
      })
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

  describe('recoverStuckJobs() batching', () => {
    /** Claim `count` jobs and backdate their leases so all look stuck. */
    async function seedStuckJobs(count: number): Promise<void> {
      for (let i = 0; i < count; i++) {
        await backend.enqueue('stuck', { i }, { maxAttempts: 5 })
        const claimed = await backend.claimNext('stuck')
        await collection.updateOne(
          { _id: claimed!.id },
          { $set: { claimedAt: new Date(Date.now() - 400000 - i * 1000) } },
        )
      }
    }

    // Regression: the sweep opened an unbounded cursor and issued one write per
    // stuck document, serially. After a mass worker death that walks the entire
    // stuck set in a single pass — while the system is already degraded.
    it('handles at most maxPerSweep jobs in one pass', async () => {
      await seedStuckJobs(5)

      const handled = await backend.recoverStuckJobs(300000, 2)

      expect(handled).toBe(2)
      expect(await collection.countDocuments({ status: 'active' })).toBe(3)
    })

    it('drains the remainder across subsequent sweeps', async () => {
      await seedStuckJobs(5)

      let total = 0
      for (let i = 0; i < 3; i++) {
        total += await backend.recoverStuckJobs(300000, 2)
      }

      expect(total).toBe(5)
      expect(await collection.countDocuments({ status: 'active' })).toBe(0)
    })

    it('recovers the longest-stuck leases first', async () => {
      await seedStuckJobs(3) // seeded oldest-lease-last

      await backend.recoverStuckJobs(300000, 1)

      // The job backdated furthest (i = 2) is the one recovered.
      const recovered = await collection.findOne({ status: 'pending' })
      expect((recovered!.data as { i: number }).i).toBe(2)
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
