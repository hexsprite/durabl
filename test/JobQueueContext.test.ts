/**
 * Regressions for the two JobContext defects found in the 2026-08-01 audit:
 *
 *  - `ctx.log()` fired a promise with no catch. A transient write failure
 *    surfaced as an unhandled rejection, which Node's default
 *    `--unhandled-rejections=throw` turns into a dead worker: one flaky log
 *    write took down the process.
 *  - Automatic heartbeats must carry the claim token and abort the handler
 *    signal when another worker owns the job or the job becomes terminal.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { JobQueue } from '../src/JobQueue'
import type { Job, JobContext } from '../src/types'

import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

/**
 * Run one job through a real processor loop and hand its context back.
 * The handler parks until the test releases it, so assertions can run against
 * a live claim rather than a finished one.
 */
async function withRunningJob(
  queue: JobQueue,
  type = 'ctxjob',
): Promise<{ ctx: JobContext; job: Job; release: () => void }> {
  let captured: { ctx: JobContext; job: Job } | undefined
  let release!: () => void
  const parked = new Promise<void>((resolve) => {
    release = resolve
  })

  queue.process(type, async (job, ctx) => {
    captured = { ctx, job }
    await parked
  })
  await queue.enqueue(type, {})
  await waitUntil(() => captured !== undefined)

  return { ...captured!, release }
}

describe('JobContext', () => {
  const queues: JobQueue[] = []

  afterEach(async () => {
    await Promise.all(queues.splice(0).map((q) => q.shutdown(1000)))
    vi.restoreAllMocks()
  })

  describe('log()', () => {
    it('does not raise an unhandled rejection when the log write fails', async () => {
      const backend = new DummyBackend()
      backend.log = async () => {
        throw new Error('simulated write failure')
      }
      const queue = new JobQueue(backend, silentLogger)
      queues.push(queue)

      const unhandled: unknown[] = []
      const onUnhandled = (err: unknown): void => {
        unhandled.push(err)
      }
      process.on('unhandledRejection', onUnhandled)
      try {
        const { ctx, release } = await withRunningJob(queue)
        ctx.log('this write will fail')
        // An unhandled rejection is reported on a later microtask+macrotask
        // turn; give it room to land before asserting it did not.
        await new Promise((r) => setTimeout(r, 50))
        release()
        expect(unhandled).toEqual([])
      } finally {
        process.off('unhandledRejection', onUnhandled)
      }
    })

    it('reports the failed log write through the injected logger', async () => {
      const backend = new DummyBackend()
      backend.log = async () => {
        throw new Error('simulated write failure')
      }
      const logger = silentLogger
      const warn = vi.spyOn(logger, 'warn')
      const queue = new JobQueue(backend, logger)
      queues.push(queue)

      const { ctx, release } = await withRunningJob(queue)
      ctx.log('this write will fail')
      await waitUntil(() => warn.mock.calls.length > 0)
      release()

      expect(warn).toHaveBeenCalledWith(
        expect.objectContaining({ err: expect.any(Error) }),
        expect.stringContaining('job log'),
      )
    })

    it('still records the entry on the happy path', async () => {
      const backend = new DummyBackend()
      const queue = new JobQueue(backend, silentLogger)
      queues.push(queue)

      const { ctx, job, release } = await withRunningJob(queue)
      ctx.log('hello')
      await waitUntil(() =>
        Boolean(backend.jobs.find((j) => j.id === job.id)?.logs.length),
      )
      release()

      expect(backend.jobs.find((j) => j.id === job.id)!.logs).toContain('hello')
    })
  })

  describe('automatic heartbeat', () => {
    it('uses this worker claim token while the lease remains live', async () => {
      const backend = new DummyBackend()
      const heartbeat = vi.spyOn(backend, 'heartbeat')
      const queue = new JobQueue(backend, silentLogger, {
        visibilityTimeoutMs: 30,
      })
      queues.push(queue)

      const { job, release } = await withRunningJob(queue)
      await waitUntil(() => heartbeat.mock.calls.length > 0)

      expect(job.claimToken).toEqual(expect.any(String))
      expect(heartbeat).toHaveBeenCalledWith(job.id, job.claimToken)
      release()
    })

    it('aborts the handler signal once another worker owns the job', async () => {
      const backend = new DummyBackend()
      const queue = new JobQueue(backend, silentLogger, {
        visibilityTimeoutMs: 30,
      })
      queues.push(queue)

      const { ctx, job, release } = await withRunningJob(queue)
      backend.jobs.find((candidate) => candidate.id === job.id)!.claimToken =
        'new-owner-token'
      await waitUntil(() => ctx.signal.aborted)

      expect(ctx.signal.aborted).toBe(true)
      release()
    })

    it('aborts the handler signal once the job becomes terminal', async () => {
      const backend = new DummyBackend()
      const queue = new JobQueue(backend, silentLogger, {
        visibilityTimeoutMs: 30,
      })
      queues.push(queue)

      const { ctx, job, release } = await withRunningJob(queue)
      backend.jobs.find((candidate) => candidate.id === job.id)!.status =
        'failed'
      await waitUntil(() => ctx.signal.aborted)

      expect(ctx.signal.aborted).toBe(true)
      release()
    })
  })
})
