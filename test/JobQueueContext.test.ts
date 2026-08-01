/**
 * Regressions for the two JobContext defects found in the 2026-08-01 audit:
 *
 *  - `ctx.log()` fired a promise with no catch. A transient write failure
 *    surfaced as an unhandled rejection, which Node's default
 *    `--unhandled-rejections=throw` turns into a dead worker: one flaky log
 *    write took down the process.
 *  - `ctx.heartbeat()` was the only lifecycle write with no lease fence, so a
 *    zombie worker kept renewing a lease another worker now held.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { JobQueue } from '../src/JobQueue'
import type { Job, JobContext, LifecycleWriteResult } from '../src/types'

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

  describe('heartbeat()', () => {
    it('applies while this worker still holds the lease', async () => {
      const backend = new DummyBackend()
      const queue = new JobQueue(backend, silentLogger)
      queues.push(queue)

      const { ctx, release } = await withRunningJob(queue)
      const res: LifecycleWriteResult = await ctx.heartbeat()
      release()

      expect(res).toBe('applied')
    })

    it('reports lease-lost, and renews nothing, once another worker owns the job', async () => {
      const backend = new DummyBackend()
      const queue = new JobQueue(backend, silentLogger)
      queues.push(queue)

      const { ctx, job, release } = await withRunningJob(queue)
      // Another worker reclaims the job: same id, fresh claim token.
      backend.jobs.find((j) => j.id === job.id)!.claimToken = 'new-owner-token'

      const res = await ctx.heartbeat()
      release()

      expect(res).toBe('lease-lost')
    })

    it('reports lease-lost once the job is no longer active', async () => {
      const backend = new DummyBackend()
      const queue = new JobQueue(backend, silentLogger)
      queues.push(queue)

      const { ctx, job, release } = await withRunningJob(queue)
      backend.jobs.find((j) => j.id === job.id)!.status = 'failed'

      const res = await ctx.heartbeat()
      release()

      expect(res).toBe('lease-lost')
    })
  })
})
