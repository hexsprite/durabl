import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import { JobQueue } from '../src/JobQueue'
import type {
  CompleteJobResult,
  FailFatalJobResult,
  FailJobResult,
} from '../src/types'

import { silentLogger } from './testLogger'

describe('ImmediateBackend', () => {
  let backend: ImmediateBackend
  let queue: JobQueue

  beforeEach(() => {
    backend = new ImmediateBackend()
    queue = new JobQueue(backend, silentLogger)
  })

  afterEach(async () => {
    await queue.shutdown(0)
  })

  describe('enqueue() without handler', () => {
    it('creates job even without handler', async () => {
      const jobId = await backend.enqueue('unknownJob', { x: 1 })

      expect(jobId).toBe('immediate-1')
      const stats = await backend.getStats()
      expect(stats.active).toBe(1) // Created as active since it tried to run
    })
  })

  describe('enqueue() with queue.process()', () => {
    it('executes handler before enqueue resolves', async () => {
      let executed = false
      queue.process('testJob', async () => {
        executed = true
      })

      await queue.enqueue('testJob', { foo: 'bar' })

      expect(executed).toBe(true)
    })

    it('passes correct job data to handler', async () => {
      let receivedData: unknown
      queue.process('testJob', async (job) => {
        receivedData = job.data
      })

      await queue.enqueue('testJob', { userId: '123', action: 'sync' })

      expect(receivedData).toEqual({ userId: '123', action: 'sync' })
    })

    it('marks the job completed when the handler returns', async () => {
      queue.process('testJob', async () => undefined)

      await queue.enqueue('testJob', {})
      const stats = await backend.getStats()

      expect(stats.completed).toBe(1)
    })

    it('marks the job failed when the handler throws', async () => {
      queue.process('testJob', async () => {
        throw new Error('handler error')
      })

      await expect(
        queue.enqueue('testJob', {}, { maxAttempts: 1 }),
      ).rejects.toThrow('handler error')
      const stats = await backend.getStats()

      expect(stats.failed).toBe(1)
    })
  })

  describe('dedupeKey behavior', () => {
    it('returns null for duplicate dedupeKey', async () => {
      const first = await backend.claimOrEnqueue(
        'job',
        { n: 1 },
        { dedupeKey: 'unique' },
      )
      const duplicate = await backend.enqueue(
        'job',
        { n: 2 },
        { dedupeKey: 'unique' },
      )

      expect(first!.id).toBe('immediate-1')
      expect(duplicate).toBeNull()
    })

    it('allows new job after previous completes', async () => {
      queue.process('job', async () => undefined)

      const id1 = await queue.enqueue(
        'job',
        { n: 1 },
        { dedupeKey: 'unique' },
      )
      const id2 = await queue.enqueue(
        'job',
        { n: 2 },
        { dedupeKey: 'unique' },
      )

      expect(id1).toBe('immediate-1')
      expect(id2).toBe('immediate-2')
    })
  })

  describe('claimOrEnqueue()', () => {
    it('returns handle for immediate execution', async () => {
      const handle = await backend.claimOrEnqueue('job', { x: 1 })

      expect(handle).not.toBeNull()
      expect(handle!.id).toBe('immediate-1')
      expect(handle!.data).toEqual({ x: 1 })
    })

    it('handle.complete() returns the completed result', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      const result: CompleteJobResult = await handle!.complete()

      expect(result).toEqual({ status: 'completed' })
      const stats = await backend.getStats()
      expect(stats.completed).toBe(1)
    })

    it('handle.fail() returns the terminal failure result', async () => {
      const handle = await backend.claimOrEnqueue('job', {}, { maxAttempts: 1 })
      const result: FailJobResult = await handle!.fail('error')

      expect(result).toEqual({ status: 'failed-terminal' })
      const stats = await backend.getStats()
      expect(stats.failed).toBe(1)
    })

    it('returns null when pending job exists with same dedupeKey', async () => {
      queue.process('job', async () => {
        throw new Error('simulated failure')
      })
      await queue
        .enqueue(
          'job',
          {},
          {
            dedupeKey: 'user:123',
            maxAttempts: 3,
          },
        )
        .catch(() => undefined)

      const handle = await backend.claimOrEnqueue(
        'job',
        {},
        {
          dedupeKey: 'user:123',
        },
      )

      expect(handle).toBeNull()
    })

    // Regression: the claimOrEnqueue handle called complete/fail WITHOUT the
    // minted claim token, so a stale handle whose job had been reclaimed by
    // another worker (new token) would still clobber it — unlike
    // MongoJobQueue.createHandle, which fences on the token.
    it('a stale handle reports lease loss after the job is reclaimed', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      // Send the job back to pending (its token is still live here), then let
      // another worker reclaim it — claimNext mints a fresh token.
      const failed: FailJobResult = await handle!.fail('lease expired')
      expect(failed).toEqual({ status: 'retry-scheduled' })
      const reclaimed = await backend.claimNext('job')
      expect(reclaimed).not.toBeNull()

      // The original handle holds the OLD token → complete must not apply.
      const result: CompleteJobResult = await handle!.complete()
      expect(result).toEqual({ status: 'lease-lost' })
      const job = await backend.findOne({ type: 'job' })
      expect(job!.status).toBe('active') // still owned by the new worker
    })

    // Regression: an un-completed handle did not stop the next caller from
    // getting one too, so this backend agreed with the (broken) production
    // behaviour instead of catching it.
    describe("dedupeScope 'pending' single-flight", () => {
      const single = {
        dedupeKey: 'reschedule:u1',
        dedupeScope: 'pending' as const,
      }

      it('refuses a second active run while the first handle is outstanding', async () => {
        const first = await backend.claimOrEnqueue('reschedule', {}, single)
        const second = await backend.claimOrEnqueue('reschedule', {}, single)

        expect(first).not.toBeNull()
        expect(second).toBeNull()
        expect((await backend.getStats()).active).toBe(1)
      })

      it('frees the slot once the active run completes', async () => {
        const first = await backend.claimOrEnqueue('reschedule', {}, single)
        await first!.complete()

        const second = await backend.claimOrEnqueue('reschedule', {}, single)
        expect(second).not.toBeNull()
      })

      it('does not block a different dedupe key', async () => {
        const a = await backend.claimOrEnqueue('reschedule', {}, single)
        const b = await backend.claimOrEnqueue(
          'reschedule',
          {},
          { dedupeKey: 'reschedule:u2', dedupeScope: 'pending' },
        )

        expect(a).not.toBeNull()
        expect(b).not.toBeNull()
      })
    })
  })

  describe('lifecycle results', () => {
    async function claimLifecycleJob() {
      const id = await backend.enqueue('lifecycle', {})
      await backend.fail(id!, 'prepare claim')
      return (await backend.claimNext('lifecycle'))!
    }

    it('complete() reports an unfenced admin completion', async () => {
      const id = await backend.enqueue('job', {})
      const result: CompleteJobResult = await backend.complete(id!)

      expect(result).toEqual({ status: 'completed' })
      expect((await backend.findOne({ id }))!.status).toBe('completed')
    })

    it('handle.fail() reports a scheduled retry', async () => {
      const handle = await backend.claimOrEnqueue('job', {}, { maxAttempts: 3 })
      const result: FailJobResult = await handle!.fail('transient')

      expect(result).toEqual({ status: 'retry-scheduled' })
      expect((await backend.findOne({ id: handle!.id }))!.status).toBe('pending')
    })

    it('failFatal() reports an immediate terminal failure', async () => {
      const id = await backend.enqueue('job', {})
      const result: FailFatalJobResult = await backend.failFatal(id!, 'fatal')

      expect(result).toEqual({ status: 'failed-terminal' })
      expect((await backend.findOne({ id }))!.status).toBe('failed')
    })

    it('reports the stored completed status to later failure operations', async () => {
      const job = await claimLifecycleJob()
      await backend.complete(job.id, job.claimToken)

      expect(await backend.fail(job.id, 'late', job.claimToken)).toEqual({
        status: 'already-terminal',
        terminalStatus: 'completed',
      })
      expect(
        await backend.failFatal(job.id, 'late', job.claimToken),
      ).toEqual({
        status: 'already-terminal',
        terminalStatus: 'completed',
      })
    })

    it('reports the stored failed status to a later completion', async () => {
      const job = await claimLifecycleJob()
      await backend.failFatal(job.id, 'fatal', job.claimToken)

      expect(await backend.complete(job.id, job.claimToken)).toEqual({
        status: 'already-terminal',
        terminalStatus: 'failed',
      })
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

  describe('completeClaimed() dedupe release', () => {
    // Regression: completeClaimed never freed the dedupe key enqueue() reserved,
    // so a key consumed by an orchestrator-style completion blocked every future
    // enqueue with that key forever.
    it('frees the reserved dedupe key so a later enqueue with that key is not blocked', async () => {
      queue.process('flow', async (job) => {
        await backend.completeClaimed(job.id, job.claimToken!)
      })

      const first = await queue.enqueue('flow', {}, { dedupeKey: 'k1' })
      expect(first).not.toBeNull()

      const second = await queue.enqueue('flow', {}, { dedupeKey: 'k1' })
      expect(second).not.toBeNull()
    })
  })

  describe('claimNext()', () => {
    it('claims pending job', async () => {
      const handle = await backend.claimOrEnqueue(
        'pendingJob',
        { n: 2 },
        { maxAttempts: 2 },
      )
      await handle!.fail('retry later')

      const claimed = await backend.claimNext<{ n: number }>('pendingJob')

      expect(claimed?.data).toEqual({ n: 2 })
      expect(claimed?.status).toBe('active')
    })
  })

  describe('findOne()', () => {
    it('does not expose the live fencing claimToken', async () => {
      // ImmediateBackend mints a claimToken on enqueue (jobs run inline); the
      // general read view must not leak it — only the claim path may see it.
      const id = await backend.enqueue('tokJob', { n: 1 })
      const found = await backend.findOne({ type: 'tokJob' })
      expect(found).not.toBeNull()
      expect(found!.id).toBe(id)
      expect(found!.claimToken).toBeUndefined()
    })
  })

  describe('getStats()', () => {
    it('returns correct counts', async () => {
      queue.process('job', async () => undefined)

      await queue.enqueue('job', {})
      await queue.enqueue('job', {})

      const stats = await backend.getStats()

      expect(stats.completed).toBe(2)
    })

    it('filters by type', async () => {
      queue.process('typeA', async () => undefined)
      queue.process('typeB', async () => undefined)

      await queue.enqueue('typeA', {})
      await queue.enqueue('typeB', {})
      await queue.enqueue('typeA', {})

      const statsA = await backend.getStats('typeA')
      const statsB = await backend.getStats('typeB')

      expect(statsA.completed).toBe(2)
      expect(statsB.completed).toBe(1)
    })
  })

  describe('reset()', () => {
    it('clears all jobs but keeps the queue processor', async () => {
      let callCount = 0
      queue.process('job', async () => {
        callCount++
      })

      await queue.enqueue('job', {})
      backend.reset()
      await queue.enqueue('job', {})

      expect(callCount).toBe(2)
      const stats = await backend.getStats()
      expect(stats.completed).toBe(1)
    })
  })

})
