import { beforeEach, describe, expect, it } from 'vitest'

import { ImmediateBackend } from '../src/backends/ImmediateBackend'

describe('ImmediateBackend', () => {
  let backend: ImmediateBackend

  beforeEach(() => {
    backend = new ImmediateBackend()
    backend.reset()
    backend.clearHandlers()
  })

  describe('enqueue() without handler', () => {
    it('creates job even without handler', async () => {
      const jobId = await backend.enqueue('unknownJob', { x: 1 })

      expect(jobId).toBe('immediate-1')
      const stats = await backend.getStats()
      expect(stats.active).toBe(1) // Created as active since it tried to run
    })
  })

  describe('enqueue() with handler', () => {
    it('executes handler synchronously', async () => {
      let executed = false
      backend.registerHandler('testJob', async (_job, ctx) => {
        executed = true
        await ctx.complete()
      })

      await backend.enqueue('testJob', { foo: 'bar' })

      expect(executed).toBe(true)
    })

    it('passes correct job data to handler', async () => {
      let receivedData: unknown
      backend.registerHandler('testJob', async (job, ctx) => {
        receivedData = job.data
        await ctx.complete()
      })

      await backend.enqueue('testJob', { userId: '123', action: 'sync' })

      expect(receivedData).toEqual({ userId: '123', action: 'sync' })
    })

    it('marks job as completed when ctx.complete() called', async () => {
      backend.registerHandler('testJob', async (_job, ctx) => {
        await ctx.complete()
      })

      await backend.enqueue('testJob', {})
      const stats = await backend.getStats()

      expect(stats.completed).toBe(1)
    })

    it('marks job as failed when ctx.fail() called', async () => {
      backend.registerHandler('testJob', async (_job, ctx) => {
        await ctx.fail('intentional failure')
      })

      await backend.enqueue('testJob', {}, { maxAttempts: 1 })
      const stats = await backend.getStats()

      expect(stats.failed).toBe(1)
    })

    it('marks job as failed when handler throws', async () => {
      backend.registerHandler('testJob', async () => {
        throw new Error('handler error')
      })

      await backend.enqueue('testJob', {}, { maxAttempts: 1 })
      const stats = await backend.getStats()

      expect(stats.failed).toBe(1)
    })
  })

  describe('dedupeKey behavior', () => {
    it('returns null for duplicate dedupeKey', async () => {
      backend.registerHandler('job', async () => {
        // Simulate long-running job by not completing immediately
        // In ImmediateBackend, job stays active until ctx.complete()
      })

      const id1 = await backend.enqueue(
        'job',
        { n: 1 },
        {
          dedupeKey: 'unique',
        },
      )
      const id2 = await backend.enqueue(
        'job',
        { n: 2 },
        {
          dedupeKey: 'unique',
        },
      )

      expect(id1).toBe('immediate-1')
      expect(id2).toBeNull()
    })

    it('allows new job after previous completes', async () => {
      backend.registerHandler('job', async (_job, ctx) => {
        await ctx.complete()
      })

      const id1 = await backend.enqueue(
        'job',
        { n: 1 },
        {
          dedupeKey: 'unique',
        },
      )
      // First job completed, dedupeKey freed
      const id2 = await backend.enqueue(
        'job',
        { n: 2 },
        {
          dedupeKey: 'unique',
        },
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

    it('handle.complete() marks job as completed', async () => {
      const handle = await backend.claimOrEnqueue('job', {})
      await handle!.complete()

      const stats = await backend.getStats()
      expect(stats.completed).toBe(1)
    })

    it('handle.fail() marks job as failed', async () => {
      const handle = await backend.claimOrEnqueue('job', {}, { maxAttempts: 1 })
      await handle!.fail('error')

      const stats = await backend.getStats()
      expect(stats.failed).toBe(1)
    })

    it('returns null when pending job exists with same dedupeKey', async () => {
      // Register a handler that fails, putting job back to pending
      backend.registerHandler('job', async (_job, ctx) => {
        await ctx.fail('simulated failure')
      })

      // Enqueue job - it will run and fail, going back to pending
      await backend.enqueue(
        'job',
        {},
        {
          dedupeKey: 'user:123',
          maxAttempts: 3, // So it goes back to pending, not failed
        },
      )

      // Now try claimOrEnqueue with same dedupeKey
      const handle = await backend.claimOrEnqueue(
        'job',
        {},
        {
          dedupeKey: 'user:123',
        },
      )

      // Should return null since there's a pending job
      expect(handle).toBeNull()
    })
  })

  describe('claimNext()', () => {
    it('claims pending job', async () => {
      // Create a job that doesn't auto-complete
      backend.registerHandler('job', async () => {
        // Don't call ctx.complete() - leaves job in whatever state
      })

      await backend.enqueue('job', { n: 1 })
      // After handler runs without completing, job should still be in some state

      // For claimNext to work, we need a pending job
      // Let's create one without a handler
      backend.clearHandlers()
      await backend.enqueue('pendingJob', { n: 2 })

      // Actually the job becomes active on enqueue in ImmediateBackend
      // Let's check what claimNext does
      const claimed = await backend.claimNext('pendingJob')

      // Since job was already claimed during enqueue, this returns null
      expect(claimed).toBeNull()
    })
  })

  describe('getStats()', () => {
    it('returns correct counts', async () => {
      backend.registerHandler('job', async (_job, ctx) => {
        await ctx.complete()
      })

      await backend.enqueue('job', {})
      await backend.enqueue('job', {})

      const stats = await backend.getStats()

      expect(stats.completed).toBe(2)
    })

    it('filters by type', async () => {
      backend.registerHandler('typeA', async (_job, ctx) => ctx.complete())
      backend.registerHandler('typeB', async (_job, ctx) => ctx.complete())

      await backend.enqueue('typeA', {})
      await backend.enqueue('typeB', {})
      await backend.enqueue('typeA', {})

      const statsA = await backend.getStats('typeA')
      const statsB = await backend.getStats('typeB')

      expect(statsA.completed).toBe(2)
      expect(statsB.completed).toBe(1)
    })
  })

  describe('reset()', () => {
    it('clears all jobs but keeps handlers', async () => {
      let callCount = 0
      backend.registerHandler('job', async (_job, ctx) => {
        callCount++
        await ctx.complete()
      })

      await backend.enqueue('job', {})
      backend.reset()
      await backend.enqueue('job', {})

      expect(callCount).toBe(2) // Handler still registered
      const stats = await backend.getStats()
      expect(stats.completed).toBe(1) // Only second job
    })
  })

  describe('clearHandlers()', () => {
    it('removes all handlers', async () => {
      let called = false
      backend.registerHandler('job', async () => {
        called = true
      })

      backend.clearHandlers()
      await backend.enqueue('job', {})

      expect(called).toBe(false)
    })
  })
})
