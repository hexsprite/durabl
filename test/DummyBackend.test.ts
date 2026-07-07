import { beforeEach, describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { approxRecordBytes } from '../src/journal/serialize'

describe('DummyBackend', () => {
  let backend: DummyBackend

  beforeEach(() => {
    backend = new DummyBackend()
  })

  describe('enqueue()', () => {
    it('records enqueued jobs', async () => {
      const jobId = await backend.enqueue('testJob', { foo: 'bar' })

      expect(jobId).toBe('dummy-1')
      expect(backend.jobs.length).toBe(1)
      expect(backend.jobs[0].id).toBe('dummy-1')
      expect(backend.jobs[0].type).toBe('testJob')
      expect(backend.jobs[0].data).toEqual({ foo: 'bar' })
      expect(backend.jobs[0].status).toBe('pending')
      expect(backend.jobs[0].priority).toBe(0)
      expect(backend.jobs[0].attempt).toBe(0)
      expect(backend.jobs[0].maxAttempts).toBe(3)
    })

    it('respects priority option', async () => {
      await backend.enqueue('job1', {}, { priority: 10 })
      await backend.enqueue('job2', {}, { priority: -5 })

      expect(backend.jobs[0].priority).toBe(10)
      expect(backend.jobs[1].priority).toBe(-5)
    })

    it('respects maxAttempts option', async () => {
      await backend.enqueue('job', {}, { maxAttempts: 5 })

      expect(backend.jobs[0].maxAttempts).toBe(5)
    })

    it('dedupeKey prevents duplicates', async () => {
      const id1 = await backend.enqueue('job', {}, { dedupeKey: 'k' })
      const id2 = await backend.enqueue('job', {}, { dedupeKey: 'k' })
      expect(id1).toBe('dummy-1')
      expect(id2).toBeNull()
      expect(backend.jobs.length).toBe(1)
      expect(backend.jobs[0].dedupeScope).toBe('pending+active') // default
    })
  })

  describe('claimOrEnqueue()', () => {
    it('creates and claims job atomically', async () => {
      const handle = await backend.claimOrEnqueue('job', { x: 1 })

      expect(handle).not.toBeNull()
      expect(handle!.id).toBe('dummy-1')
      expect(handle!.data).toEqual({ x: 1 })
      expect(backend.jobs[0].status).toBe('active')
      expect(backend.jobs[0].attempt).toBe(1)
    })

    it('returns null when pending job exists with same dedupeKey', async () => {
      await backend.enqueue('job', {}, { dedupeKey: 'user:123' })
      const handle = await backend.claimOrEnqueue(
        'job',
        {},
        {
          dedupeKey: 'user:123',
        },
      )
      expect(handle).toBeNull()
      expect(backend.jobs.length).toBe(1)
    })

    it('handle.complete() marks job as completed', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      await handle!.complete()

      expect(backend.jobs[0].status).toBe('completed')
    })

    it('handle.fail() marks job as failed', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      await handle!.fail('something broke')

      expect(backend.jobs[0].status).toBe('failed')
      expect(backend.jobs[0].logs).toContain('Failed: something broke')
    })

    it('handle.log() adds log entry', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      handle!.log('processing started')

      expect(backend.jobs[0].logs).toContain('processing started')
    })

    // Regression: the handle mutated status directly with no lease fence, so a
    // stale handle whose job had been reclaimed (new token) could still clobber
    // it — MongoJobQueue.createHandle fences on the token.
    it('a stale handle is a fenced no-op after the job is reclaimed', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      const stored = backend.jobs.find((j) => j.id === handle!.id)!
      // Another worker reclaims: new token, still active.
      stored.claimToken = 'new-owner-token'

      await handle!.complete()
      expect(stored.status).toBe('active') // stale handle did not complete it
    })
  })

  describe('claimNext()', () => {
    it('claims oldest pending job', async () => {
      await backend.enqueue('job', { n: 1 })
      await backend.enqueue('job', { n: 2 })

      const job = await backend.claimNext('job')

      expect(job).not.toBeNull()
      expect(job!.data).toEqual({ n: 1 })
      expect(job!.status).toBe('active')
      expect(job!.attempt).toBe(1)
    })

    it('returns null when no pending jobs', async () => {
      const job = await backend.claimNext('job')
      expect(job).toBeNull()
    })

    it('returns null when only active jobs exist', async () => {
      await backend.claimOrEnqueue('job', {})

      const job = await backend.claimNext('job')
      expect(job).toBeNull()
    })
  })

  describe('complete()', () => {
    it('marks job as completed', async () => {
      await backend.enqueue('job', {})
      const job = await backend.claimNext('job')
      await backend.complete(job!.id)

      expect(backend.jobs[0].status).toBe('completed')
    })
  })

  describe('fail()', () => {
    it('returns job to pending if attempts remain', async () => {
      await backend.enqueue('job', {}, { maxAttempts: 3 })
      const job = await backend.claimNext('job')
      await backend.fail(job!.id, 'error')

      expect(backend.jobs[0].status).toBe('pending')
      expect(backend.jobs[0].attempt).toBe(1)
    })

    it('marks job as failed when max attempts reached', async () => {
      await backend.enqueue('job', {}, { maxAttempts: 1 })
      const job = await backend.claimNext('job')
      await backend.fail(job!.id, 'error')

      expect(backend.jobs[0].status).toBe('failed')
    })
  })

  describe('failFatal()', () => {
    it('marks job as failed immediately', async () => {
      await backend.enqueue('job', {}, { maxAttempts: 10 })
      const job = await backend.claimNext('job')
      await backend.failFatal(job!.id, 'fatal error')

      expect(backend.jobs[0].status).toBe('failed')
      expect(backend.jobs[0].logs).toContain('Fatal: fatal error')
    })
  })

  describe('log() journal accounting', () => {
    // Regression: log() added approxRecordBytes(message) — the bare string —
    // while MongoJobQueue.logWrite and ImmediateBackend.log count the full
    // {timestamp, message} entry. DummyBackend under-counted, drifting the
    // size-guard fidelity away from prod.
    it('counts the full {timestamp, message} entry, not the bare string', async () => {
      const id = (await backend.enqueue('t', {})) as string
      await backend.log(id, 'hello')

      const job = backend.jobs.find((j) => j.id === id)!
      const entryBytes = approxRecordBytes({
        timestamp: new Date(),
        message: 'hello',
      })
      const bareBytes = approxRecordBytes('hello')
      expect(job.journalBytes).toBe(entryBytes) // entry-shaped, matches prod
      expect(job.journalBytes).toBeGreaterThan(bareBytes) // not the bare string
    })
  })

  describe('getStats()', () => {
    it('returns correct counts', async () => {
      await backend.enqueue('job', {})
      await backend.enqueue('job', {})
      await backend.claimOrEnqueue('job', {})
      const job = await backend.claimNext('job')
      await backend.complete(job!.id)

      const stats = await backend.getStats()

      expect(stats).toEqual({
        pending: 1,
        active: 1,
        completed: 1,
        failed: 0,
      })
    })

    it('filters by type', async () => {
      await backend.enqueue('typeA', {})
      await backend.enqueue('typeB', {})

      const statsA = await backend.getStats('typeA')
      const statsB = await backend.getStats('typeB')

      expect(statsA.pending).toBe(1)
      expect(statsB.pending).toBe(1)
    })
  })

  describe('reset()', () => {
    it('clears all jobs and resets ID counter', async () => {
      await backend.enqueue('job', {})
      await backend.enqueue('job', {})
      backend.reset()
      await backend.enqueue('job', {})
      expect(backend.jobs.length).toBe(1)
      expect(backend.jobs[0].id).toBe('dummy-1')
    })
  })

  describe('helper methods', () => {
    it('getJobsByType/Status filter correctly', async () => {
      await backend.enqueue('typeA', {})
      await backend.enqueue('typeB', {})
      await backend.claimOrEnqueue('typeA', {})
      expect(backend.getJobsByType('typeA').length).toBe(2)
      expect(backend.getJobsByStatus('pending').length).toBe(2)
      expect(backend.getJobsByStatus('active').length).toBe(1)
    })
  })

  describe('lease-fenced lifecycle writes (zombie worker fencing)', () => {
    /** Claim a job, then simulate a reaper reclaim: the job stays active but
     * under a NEW claim token owned by another worker. Returns the stale
     * token the "zombie" worker still holds. */
    async function claimThenReclaim(): Promise<{
      id: string
      staleToken: string
    }> {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      const stored = backend.jobs.find((j) => j.id === job!.id)!
      stored.claimToken = 'new-owner-token'
      return { id: job!.id, staleToken: job!.claimToken! }
    }

    // Regression (du-4ft): fail() was unfenced — a zombie worker's failure
    // report clobbered a job the reaper had already handed to a live worker,
    // resetting it to pending/failed under the new owner's feet.
    it('a zombie fail() with a stale token does not clobber the reclaimed job', async () => {
      const { id, staleToken } = await claimThenReclaim()
      const res = await backend.fail(id, 'zombie says boom', staleToken)
      expect(res).toBe('lease-lost')
      const stored = backend.jobs.find((j) => j.id === id)!
      expect(stored.status).toBe('active') // still owned by the new worker
      expect(stored.logs).toHaveLength(0) // nothing destructive happened
    })

    // Regression (du-4ft): failFatal() was unfenced — a zombie could mark a
    // live, reclaimed job terminally failed.
    it('a zombie failFatal() with a stale token does not clobber the reclaimed job', async () => {
      const { id, staleToken } = await claimThenReclaim()
      const res = await backend.failFatal(id, 'zombie fatal', staleToken)
      expect(res).toBe('lease-lost')
      const stored = backend.jobs.find((j) => j.id === id)!
      expect(stored.status).toBe('active')
      expect(stored.logs).toHaveLength(0)
    })

    it('a zombie complete() with a stale token does not complete the reclaimed job', async () => {
      const { id, staleToken } = await claimThenReclaim()
      const res = await backend.complete(id, staleToken)
      expect(res).toBe('lease-lost')
      expect(backend.jobs.find((j) => j.id === id)!.status).toBe('active')
    })

    it('fenced writes with the live token still apply', async () => {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      expect(await backend.complete(job!.id, job!.claimToken)).toBe('applied')
      expect(backend.jobs.find((j) => j.id === job!.id)!.status).toBe(
        'completed',
      )
    })

    it('unfenced writes (no token) keep legacy behavior and report applied', async () => {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      expect(await backend.complete(job!.id)).toBe('applied')
      expect(backend.jobs.find((j) => j.id === job!.id)!.status).toBe(
        'completed',
      )
    })

    // Regression (du-4ft): completeClaimed matched status:'active' only. The
    // reaper marks an attempt-exhausted job 'failed' WITHOUT clearing its
    // claimToken; a worker that then finished all steps got 'lease-lost' and
    // the completed work was permanently lost.
    it('completeClaimed flips a reaper-exhausted-but-actually-complete run to completed', async () => {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      const stored = backend.jobs.find((j) => j.id === job!.id)!
      // Simulate the reaper: attempt-exhausted → failed, token NOT cleared.
      stored.status = 'failed'
      const res = await backend.completeClaimed(job!.id, job!.claimToken!)
      expect(res).toBe('completed')
      expect(stored.status).toBe('completed')
    })

    it('completeClaimed on a reaper-failed job with a DIFFERENT token stays lease-lost', async () => {
      await backend.enqueue('t', {})
      const job = await backend.claimNext('t')
      const stored = backend.jobs.find((j) => j.id === job!.id)!
      stored.status = 'failed'
      stored.claimToken = 'reclaimed-by-someone-else'
      const res = await backend.completeClaimed(job!.id, job!.claimToken!)
      expect(res).toBe('lease-lost')
      expect(stored.status).toBe('failed')
    })
  })
})
