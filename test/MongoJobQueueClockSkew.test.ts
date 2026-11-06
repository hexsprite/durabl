/**
 * Lease and schedule times under clock skew between hosts.
 *
 * Symptom: every host stamped `claimedAt` and `runAt` with its own clock. If a
 * worker's clock ran behind the reaper's by more than about 2/3 of the
 * visibility timeout, the reaper treated the worker's live, heartbeated job as
 * stale and reclaimed it, so the job ran twice. A delayed job enqueued by a
 * skewed host also fired early or late by the skew. The fix takes every lease
 * and schedule time from the Mongo server clock.
 *
 * Each test acts as two hosts in one process: `setClockSkew` shifts the
 * process clock before a host's calls. A host measures its server-clock
 * offset in `startup()`, so each host starts up under its own skew.
 */
import type { Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import { JobQueue } from '../src/JobQueue'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'
import {
  installSkewableClock,
  realNow,
  setClockSkew,
} from './skewedClock'
import { silentLogger } from './testLogger'

const FIVE_MIN = 5 * 60_000
const visibilityTimeoutMs = 60_000

describe('MongoJobQueue under clock skew', () => {
  let db: Db
  let collectionName: string
  let restoreClock: () => void
  const started: MongoJobQueue[] = []

  /** Start a backend as a host whose clock is off by `skewMs`. */
  async function host(skewMs: number): Promise<MongoJobQueue> {
    setClockSkew(skewMs)
    const backend = new MongoJobQueue({ db, collectionName, logger: silentLogger })
    await backend.startup()
    started.push(backend)
    return backend
  }

  beforeEach(async () => {
    ;({ db } = await getMongo())
    collectionName = uniqueCollectionName('clock_skew_jobs')
    restoreClock = installSkewableClock()
  })

  afterEach(async () => {
    restoreClock()
    for (const backend of started.splice(0)) await backend.shutdown()
    await db.collection(collectionName).drop().catch(() => {
      /* already gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it.each([
    ['worker 5 min behind, reaper correct', -FIVE_MIN, 0],
    ['worker correct, reaper 5 min ahead', 0, FIVE_MIN],
  ])(
    'does not reap a live heartbeated job when the %s',
    async (_label, workerSkew, reaperSkew) => {
      const worker = await host(workerSkew)
      const reaper = await host(reaperSkew)

      setClockSkew(workerSkew)
      await worker.enqueue('t', { n: 1 })
      const job = await worker.claimNext('t')
      expect(job).not.toBeNull()
      expect(await worker.heartbeat(job!.id, job!.claimToken)).toBe('applied')

      setClockSkew(reaperSkew)
      // Before the fix the worker's claimedAt was 5 min off the reaper's
      // clock, past the 60s cutoff, so this reaped a live job.
      expect(await reaper.recoverStuckJobs(visibilityTimeoutMs)).toBe(0)

      const doc = await worker.getCollection().findOne({ _id: job!.id })
      expect(doc?.status).toBe('active')
      expect(doc?.claimToken).toBe(job!.claimToken)
    },
  )

  it.each([
    ['correct reaper', 0],
    ['reaper 5 min behind', -FIVE_MIN],
  ])('still reaps a truly stale job with a %s', async (_label, reaperSkew) => {
    const worker = await host(-FIVE_MIN)
    const reaper = await host(reaperSkew)

    setClockSkew(-FIVE_MIN)
    await worker.enqueue('t', { n: 1 })
    const job = await worker.claimNext('t')
    expect(job).not.toBeNull()

    // The test's mongod runs on this machine, so the real clock is the
    // server clock. Backdate the lease past the cutoff in server time.
    await worker.getCollection().updateOne(
      { _id: job!.id },
      { $set: { claimedAt: new Date(realNow() - visibilityTimeoutMs - 30_000) } },
    )

    setClockSkew(reaperSkew)
    // Before the fix a reaper running behind computed its cutoff 5 min too
    // early and never reaped this job.
    expect(await reaper.recoverStuckJobs(visibilityTimeoutMs)).toBe(1)
    const doc = await worker.getCollection().findOne({ _id: job!.id })
    expect(doc?.status).toBe('pending')
  })

  it.each([
    ['5 min behind', -FIVE_MIN],
    ['5 min ahead', FIVE_MIN],
  ])(
    'a delayed job enqueued by a host %s runs on server time',
    async (_label, enqueuerSkew) => {
      const enqueuer = await host(enqueuerSkew)
      const claimer = await host(0)
      const delayMs = 1500

      setClockSkew(enqueuerSkew)
      const enqueuedAt = realNow()
      const id = await enqueuer.enqueue('t', { n: 1 }, { delay: delayMs })

      const doc = await enqueuer.getCollection().findOne({ _id: id! })
      // Before the fix runAt carried the enqueuer's skew: 5 min early or late.
      expect(Math.abs(doc!.runAt.getTime() - (enqueuedAt + delayMs))).toBeLessThan(
        1000,
      )

      setClockSkew(0)
      expect(await claimer.claimNext('t')).toBeNull()
      await new Promise((resolve) => setTimeout(resolve, delayMs + 300))
      const claimed = await claimer.claimNext('t')
      expect(claimed?.id).toBe(id)
    },
  )

  it.each([
    ['5 min behind', -FIVE_MIN],
    ['5 min ahead', FIVE_MIN],
  ])(
    'the JobQueue self-fence fires one lease after the claim on a host %s',
    async (_label, workerSkew) => {
      // Once claimedAt is server time, a local deadline built from claimedAt
      // would carry the host's skew: a host ahead aborts at once, a host
      // behind keeps running 5 min past its lease. The deadline must start
      // from the local time of the claim.
      const backend = await host(workerSkew)
      const leaseMs = 1500
      const queue = new JobQueue(backend, silentLogger, {
        visibilityTimeoutMs: leaseMs,
      })

      let claimedAt = 0
      let abortedAfterMs = -1
      queue.process(
        't',
        async (_job, ctx) => {
          claimedAt = realNow()
          await new Promise<void>((resolve) => {
            ctx.signal.addEventListener(
              'abort',
              () => {
                abortedAfterMs = realNow() - claimedAt
                resolve()
              },
              { once: true },
            )
          })
        },
        // No renewal inside the test window, so only the deadline can abort.
        { heartbeatIntervalMs: 60_000, pollInterval: 20 },
      )
      try {
        await queue.enqueue('t', { n: 1 })
        await new Promise((resolve) => setTimeout(resolve, leaseMs + 1000))
        expect(abortedAfterMs).toBeGreaterThan(leaseMs - 300)
        expect(abortedAfterMs).toBeLessThan(leaseMs + 300)
      } finally {
        await queue.shutdown(1000)
      }
    },
  )
})
