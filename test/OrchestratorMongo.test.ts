/**
 * Durability / crash-resume / lease-fencing for the Orchestrator against a real
 * MongoJobQueue (in-memory replica set via mongodb-memory-server). These are the
 * acceptance criteria that the in-memory suite cannot prove: atomic conditional
 * append, claim-token fencing, and resume across the real retry/backoff path.
 */
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { Collection, Db } from 'mongodb'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import { JobQueue } from '../src/JobQueue'
import { JournalTooLarge, NondeterminismError } from '../src/journal/errors'
import { Orchestrator } from '../src/orchestrator/Orchestrator'
import type { JobDoc, StepRecord } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'
import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

describe('Orchestrator durability (Mongo)', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>
  let queue: JobQueue
  let orch: Orchestrator

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('orch_jobs'),
    })
    collection = backend.getCollection()
    await backend.startup()
    queue = new JobQueue(backend, silentLogger)
    orch = new Orchestrator(queue, silentLogger)
  })

  afterEach(async () => {
    await queue.shutdown(2000)
    await backend.shutdown()
    await collection.drop().catch(() => {
      /* ignore */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  const statusOf = async (id: string): Promise<string | undefined> =>
    (await collection.findOne({ _id: id }))?.status

  // Fast retries so the resume path doesn't wait on the default 1s+ backoff.
  const fastEnqueue = { backoffDelay: 10, backoffMaxDelay: 20 }
  const fast = { pollInterval: 50 }

  it('claimNext mints a fresh claimToken on every claim', async () => {
    await backend.enqueue('t', {})
    const a = await backend.claimNext('t')
    expect(a?.claimToken).toEqual(expect.any(String))
    // return it to pending, re-claim → different token
    await collection.updateOne(
      { _id: a!.id },
      { $set: { status: 'pending' } },
    )
    const b = await backend.claimNext('t')
    expect(b?.claimToken).toEqual(expect.any(String))
    expect(b?.claimToken).not.toBe(a?.claimToken)
  })

  it('resumes from the journal across a real crash + retry', async () => {
    const effects = { a: 0, b: 0 }
    let crashed = false
    orch.define(
      'flow',
      async (_job, octx) => {
        await octx.step('a', async () => {
          effects.a++
          return 'ra'
        })
        if (!crashed) {
          crashed = true
          throw new Error('boom')
        }
        await octx.step('b', async () => {
          effects.b++
          return 'rb'
        })
      },
      fast,
    )

    const id = (await queue.enqueue('flow', {}, fastEnqueue)) as string
    await waitUntil(async () => (await statusOf(id)) === 'completed')

    expect(effects.a).toBe(1) // journaled, not re-run on resume
    expect(effects.b).toBe(1)
    const doc = await collection.findOne({ _id: id })
    expect(doc?.steps?.map((s) => s.name).sort()).toEqual(['a', 'b'])
  })

  it('divergence on resume fails fatally with no further retry', async () => {
    let attempt = 0
    orch.define(
      'div',
      async (_job, octx) => {
        attempt++
        await octx.step(attempt === 1 ? 'first' : 'second', async () => 'x')
        if (attempt === 1) throw new Error('crash')
      },
      fast,
    )

    const id = (await queue.enqueue('div', {}, fastEnqueue)) as string
    await waitUntil(async () => (await statusOf(id)) === 'failed')

    const doc = await collection.findOne({ _id: id })
    expect(doc?.failReason).toMatch(/nondeterminism/i)
    expect(attempt).toBe(2)
  })

  describe('claim-token fencing (stale worker)', () => {
    const rec: StepRecord = { seq: 0, name: 's', result: 'r', ts: new Date(0) }

    it('appendStep with a stale token returns lease-lost and writes nothing', async () => {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      // Simulate the reaper reclaiming under a new token while keeping attempt.
      await collection.updateOne(
        { _id: job!.id },
        { $set: { claimToken: 'new-token' } },
      )
      const result = await backend.appendStep(job!.id, job!.claimToken!, rec)
      expect(result).toEqual({ status: 'lease-lost' })
      const doc = await collection.findOne({ _id: job!.id })
      expect(doc?.steps ?? []).toHaveLength(0)
    })

    it('completeClaimed with a stale token returns lease-lost; job not completed', async () => {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      await collection.updateOne(
        { _id: job!.id },
        { $set: { claimToken: 'new-token' } },
      )
      const result = await backend.completeClaimed(job!.id, job!.claimToken!)
      expect(result).toBe('lease-lost')
      expect(await statusOf(job!.id)).toBe('active') // still owned by new token
    })

    it('heartbeatClaimed with a stale token does not refresh claimedAt', async () => {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      const before = (await collection.findOne({ _id: job!.id }))?.claimedAt
      await collection.updateOne(
        { _id: job!.id },
        { $set: { claimToken: 'new-token' } },
      )
      await new Promise((r) => setTimeout(r, 5))
      const result = await backend.heartbeatClaimed(job!.id, job!.claimToken!)
      expect(result).toBe('lease-lost')
      const after = (await collection.findOne({ _id: job!.id }))?.claimedAt
      expect(after?.getTime()).toBe(before?.getTime())
    })
  })

  describe('completeClaimed vs the reaper (du-4ft)', () => {
    // Regression: recoverStuckJobs marks an attempt-exhausted job
    // status:'failed' WITHOUT clearing claimToken. completeClaimed filtered on
    // status:'active' only, so a worker that had actually finished every step
    // got 'lease-lost' and the completed work was permanently lost.
    it('flips a reaper-exhausted-but-actually-complete run to completed and clears failReason', async () => {
      await backend.enqueue('t', {}, { maxAttempts: 1 })
      const job = await backend.claimNext('t')
      const token = job!.claimToken!
      await backend.appendStep(job!.id, token, {
        seq: 0,
        name: 's',
        result: 'r',
        ts: new Date(0),
      })
      // Real reaper path: lease expired, attempt (1) >= maxAttempts (1) →
      // terminal failed, claimToken left in place.
      await new Promise((r) => setTimeout(r, 10))
      expect(await backend.recoverStuckJobs(5)).toBe(1)
      let doc = await collection.findOne({ _id: job!.id })
      expect(doc?.status).toBe('failed')
      expect(doc?.claimToken).toBe(token)

      // The worker finished all steps and reports completion under ITS token.
      expect(await backend.completeClaimed(job!.id, token)).toBe('completed')
      doc = await collection.findOne({ _id: job!.id })
      expect(doc?.status).toBe('completed')
      expect(doc?.failReason).toBeUndefined() // stale reaper verdict cleared
      expect(doc?.failedAt).toBeUndefined()
    })

    it('still returns lease-lost on a reaper-failed job under a DIFFERENT token', async () => {
      await backend.enqueue('t', {}, { maxAttempts: 1 })
      const job = await backend.claimNext('t')
      await new Promise((r) => setTimeout(r, 10))
      expect(await backend.recoverStuckJobs(5)).toBe(1)
      // A genuinely reclaimed job would carry a fresh token; the zombie's
      // stale one must not resurrect it.
      const res = await backend.completeClaimed(job!.id, 'some-other-token')
      expect(res).toBe('lease-lost')
      expect(await statusOf(job!.id)).toBe('failed')
    })
  })

  it('readSteps returns records ordered by seq regardless of append order', async () => {
    await backend.enqueue('t', {})
    const job = await backend.claimNext('t')
    const token = job!.claimToken!
    // Append seq 2 before seq 1 (fan-out completion order).
    await backend.appendStep(job!.id, token, {
      seq: 2,
      name: 's2',
      result: 'b',
      ts: new Date(0),
    })
    await backend.appendStep(job!.id, token, {
      seq: 1,
      name: 's1',
      result: 'a',
      ts: new Date(0),
    })
    const steps = await backend.readSteps(job!.id)
    expect(steps.map((s) => s.seq)).toEqual([1, 2])
  })

  it('appendStep is idempotent on driver-retry (same seq + name)', async () => {
    await backend.enqueue('t', {})
    const job = await backend.claimNext('t')
    const token = job!.claimToken!
    const rec: StepRecord = { seq: 0, name: 's', result: 'r', ts: new Date(0) }
    expect(await backend.appendStep(job!.id, token, rec)).toEqual({
      status: 'appended',
    })
    // already-recorded carries the existing record so the caller can resolve
    // the driver-retry ambiguity without re-reading the journal.
    expect(await backend.appendStep(job!.id, token, rec)).toMatchObject({
      status: 'already-recorded',
      existing: { seq: 0, name: 's', result: 'r' },
    })
    const steps = await backend.readSteps(job!.id)
    expect(steps).toHaveLength(1)
  })

  it('appendStep throws NondeterminismError on same seq, different name', async () => {
    await backend.enqueue('t', {})
    const job = await backend.claimNext('t')
    const token = job!.claimToken!
    await backend.appendStep(job!.id, token, {
      seq: 0,
      name: 'a',
      result: 'r',
      ts: new Date(0),
    })
    await expect(
      backend.appendStep(job!.id, token, {
        seq: 0,
        name: 'b',
        result: 'r',
        ts: new Date(0),
      }),
    ).rejects.toBeInstanceOf(NondeterminismError)
  })

  // Regression (du-2ap): the JournalTooLarge soft cap checks a PRE-write
  // snapshot and the $push filter has no size predicate, so concurrent fan-out
  // appends (Promise.all over octx.step, sanctioned in v1) sharing one claim
  // token could each pass the soft check and blow Mongo's 16MB BSON cap at
  // write time — surfacing a raw MongoServerError (code 10334) instead of the
  // typed JournalTooLarge the spec (§8.1) promises.
  it('concurrent fan-out appends that blow the 16MB doc cap surface JournalTooLarge, not a raw MongoServerError', async () => {
    // Soft cap ABOVE the 16MB hard cap so the pre-write guard never fires and
    // the driver/server error path is the one under test.
    const bigBackend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('orch_jobs_big'),
      journalSoftLimitBytes: 64_000_000,
    })
    await bigBackend.startup()
    const bigCollection = bigBackend.getCollection()
    try {
      await bigBackend.enqueue('t', {})
      const job = await bigBackend.claimNext('t')
      const token = job!.claimToken!

      // 3 × ~6.5MB: any two fit (13MB), all three exceed the 16MB doc cap.
      const payload = 'x'.repeat(6_500_000)
      const results = await Promise.allSettled(
        [0, 1, 2].map((seq) =>
          bigBackend.appendStep(job!.id, token, {
            seq,
            name: `s${seq}`,
            result: payload,
            ts: new Date(0),
          }),
        ),
      )

      const rejected = results.filter((r) => r.status === 'rejected')
      expect(rejected.length).toBeGreaterThanOrEqual(1)
      for (const r of rejected) {
        expect((r as PromiseRejectedResult).reason).toBeInstanceOf(
          JournalTooLarge,
        )
      }
    } finally {
      await bigBackend.shutdown()
      await bigCollection.drop().catch(() => {
        /* ignore */
      })
    }
  })

  it('appendStep bumps claimedAt (free lease extension)', async () => {
    await backend.enqueue('t', {})
    const job = await backend.claimNext('t')
    const before = (await collection.findOne({ _id: job!.id }))!.claimedAt!
    await new Promise((r) => setTimeout(r, 10))
    await backend.appendStep(job!.id, job!.claimToken!, {
      seq: 0,
      name: 's',
      result: 'r',
      ts: new Date(0),
    })
    const after = (await collection.findOne({ _id: job!.id }))!.claimedAt!
    expect(after.getTime()).toBeGreaterThan(before.getTime())
  })

  it('a long step holds the lease against the reaper via heartbeat', async () => {
    // visibilityTimeout shorter than the step; the heartbeat must keep the job
    // from being recovered mid-run.
    const vis = 300
    queue = new JobQueue(backend, silentLogger, { visibilityTimeoutMs: vis })
    orch = new Orchestrator(queue, silentLogger)

    let reaped = false
    orch.define(
      'slow',
      async (_job, octx) => {
        await octx.step('long', async () => {
          // Run well past the visibility timeout while the reaper sweeps.
          for (let i = 0; i < 6; i++) {
            await new Promise((r) => setTimeout(r, 100))
            const handled = await backend.recoverStuckJobs(vis)
            if (handled > 0) reaped = true
          }
          return 'done'
        })
      },
      // stepTimeoutMs must exceed the step's real duration — the lease is held
      // by the heartbeat, not by the (short) visibility window.
      { ...fast, visibilityTimeoutMs: vis, stepTimeoutMs: 3000 },
    )

    const id = (await queue.enqueue('slow', {}, fastEnqueue)) as string
    await waitUntil(async () => (await statusOf(id)) === 'completed', 8000)
    expect(reaped).toBe(false) // heartbeat kept the lease alive
  })

  it('rejects a visibilityTimeoutMs override looser than the reaper timeout', () => {
    const q = new JobQueue(backend, silentLogger, { visibilityTimeoutMs: 1000 })
    const o = new Orchestrator(q, silentLogger)
    expect(() =>
      o.define('x', async () => {}, { visibilityTimeoutMs: 5000 }),
    ).toThrow(/looser/)
  })
})
