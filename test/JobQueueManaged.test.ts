import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  type Mock,
  vi,
} from 'vitest'

import { FatalJobError, ImmediateBackend, JobQueue } from '../src'
import { DummyBackend } from '../src/backends/DummyBackend'
import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import type {
  CompleteJobResult,
  Job,
  JobEvent,
  JobHandle,
  QueueStats,
} from '../src/types'

import { silentLogger } from './testLogger'

function deferred<T = void>(): {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (reason?: unknown) => void
} {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const activeJob = (over: Partial<Job> = {}): Job => ({
  id: 'j1',
  type: 'sync',
  data: { n: 1 },
  status: 'active',
  attempt: 1,
  maxAttempts: 3,
  priority: 0,
  dedupeKey: 'account:1',
  dedupeScope: 'pending',
  runAt: new Date(),
  createdAt: new Date(),
  claimedAt: new Date(),
  claimToken: 'claim-1',
  ...over,
})

function makeBackend(
  over: Partial<IJobQueueBackend> = {},
): IJobQueueBackend {
  return {
    enqueue: vi.fn().mockResolvedValue('j1'),
    claimOrEnqueue: vi.fn().mockResolvedValue(null),
    claimNext: vi.fn().mockResolvedValue(null),
    claimNextByKey: vi.fn().mockResolvedValue(null),
    complete: vi.fn().mockResolvedValue({ status: 'completed' }),
    fail: vi.fn().mockResolvedValue({ status: 'retry-scheduled' }),
    failFatal: vi.fn().mockResolvedValue({ status: 'failed-terminal' }),
    release: vi.fn().mockResolvedValue({ status: 'released' }),
    log: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue('applied'),
    hasOutstanding: vi.fn().mockResolvedValue(false),
    findOne: vi.fn().mockResolvedValue(null),
    getStats: vi.fn().mockResolvedValue({
      pending: 0,
      active: 0,
      completed: 0,
      failed: 0,
    } as QueueStats),
    startup: vi.fn().mockResolvedValue(undefined),
    shutdown: vi.fn().mockResolvedValue(undefined),
    resetStorage: vi.fn().mockResolvedValue(undefined),
    ...over,
  } as unknown as IJobQueueBackend
}

interface HandleHarness<T> {
  handle: JobHandle<T>
  complete: Mock
  fail: Mock
  failFatal: Mock
  heartbeat: Mock
  release: Mock
}

function makeHandle<T>(
  data: T,
  over: Partial<Job> = {},
): HandleHarness<T> {
  const job = activeJob({ data, ...over })
  const complete = vi.fn().mockResolvedValue({ status: 'completed' })
  const fail = vi.fn().mockResolvedValue({ status: 'retry-scheduled' })
  const failFatal = vi.fn().mockResolvedValue({ status: 'failed-terminal' })
  const heartbeat = vi.fn().mockResolvedValue('applied')
  const release = vi.fn().mockResolvedValue({ status: 'released' })
  const handle = {
    ...job,
    complete,
    fail,
    failFatal,
    heartbeat,
    release,
    log: vi.fn(),
  } as unknown as JobHandle<T>
  return { handle, complete, fail, failFatal, heartbeat, release }
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise
  } catch (error) {
    return error
  }
  throw new Error('expected promise to reject')
}

describe('JobQueue managed execution', () => {
  it('passes only managed context and completes exactly once on return', async () => {
    const backend = makeBackend()
    const queue = new JobQueue(backend, silentLogger)
    const run = makeHandle({ n: 1 })

    await queue.runClaimed(run.handle, (job, ctx) => {
      expect(job).not.toBe(run.handle)
      expect(job.data).toEqual({ n: 1 })
      expect(job).not.toHaveProperty('complete')
      expect(job).not.toHaveProperty('fail')
      expect(Object.keys(ctx).sort()).toEqual(['log', 'signal'])
      expect(ctx.signal).toBeInstanceOf(AbortSignal)
    })

    expect(run.complete).toHaveBeenCalledTimes(1)
    expect(run.fail).not.toHaveBeenCalled()
    expect(run.failFatal).not.toHaveBeenCalled()
  })

  it.each([
    [{ status: 'retry-scheduled' }, false],
    [{ status: 'failed-terminal' }, true],
  ] as const)(
    'records an ordinary throw before rejecting (%s)',
    async (failResult, terminal) => {
      const events: JobEvent[] = []
      const queue = new JobQueue(makeBackend(), silentLogger, {
        onJobEvent: (event) => events.push(event),
      })
      const run = makeHandle({ n: 1 }, {
        attempt: terminal ? 1 : 3,
        maxAttempts: terminal ? 3 : 3,
      })
      run.fail.mockResolvedValue(failResult)
      const handlerError = new Error('retry me')

      await expect(
        queue.runClaimed(run.handle, async () => {
          throw handlerError
        }),
      ).rejects.toBe(handlerError)

      expect(run.fail).toHaveBeenCalledWith('retry me')
      expect(run.complete).not.toHaveBeenCalled()
      expect(events.find((event) => event.kind === 'failed')).toMatchObject({
        kind: 'failed',
        reason: 'retry me',
        terminal,
      })
    },
  )

  it('records FatalJobError through failFatal before rejecting', async () => {
    const queue = new JobQueue(makeBackend(), silentLogger)
    const run = makeHandle({ n: 1 })
    const cause = new Error('decoder failed')
    const fatal = new FatalJobError('poison payload', { cause })

    await expect(
      queue.runClaimed(run.handle, async () => {
        throw fatal
      }),
    ).rejects.toBe(fatal)

    expect(fatal.cause).toBe(cause)
    expect(run.failFatal).toHaveBeenCalledWith('poison payload')
    expect(run.fail).not.toHaveBeenCalled()
    expect(run.complete).not.toHaveBeenCalled()
  })

  it('does not turn a completion write error into a failure', async () => {
    const queue = new JobQueue(makeBackend(), silentLogger)
    const run = makeHandle({ n: 1 })
    const writeError = new Error('completion write failed')
    run.complete.mockRejectedValue(writeError)

    await expect(
      queue.runClaimed(run.handle, async () => undefined),
    ).rejects.toBe(writeError)

    expect(run.complete).toHaveBeenCalledTimes(1)
    expect(run.fail).not.toHaveBeenCalled()
    expect(run.failFatal).not.toHaveBeenCalled()
  })

  it('preserves handler then failure-write errors in an AggregateError', async () => {
    const queue = new JobQueue(makeBackend(), silentLogger)
    const run = makeHandle({ n: 1 })
    const handlerError = new Error('handler failed')
    const writeError = new Error('failure write failed')
    run.fail.mockRejectedValue(writeError)

    const error = await rejection(
      queue.runClaimed(run.handle, async () => {
        throw handlerError
      }),
    )

    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).errors).toEqual([handlerError, writeError])
    expect(run.complete).not.toHaveBeenCalled()
  })

  describe('heartbeat and cancellation', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it('heartbeats every visibilityTimeoutMs / 3 and clears its timer', async () => {
      const queue = new JobQueue(makeBackend(), silentLogger, {
        visibilityTimeoutMs: 900,
      })
      const run = makeHandle({ n: 1 })
      const handler = deferred<void>()
      const running = queue.runClaimed(run.handle, () => handler.promise)

      await vi.advanceTimersByTimeAsync(299)
      expect(run.heartbeat).not.toHaveBeenCalled()
      await vi.advanceTimersByTimeAsync(1)
      expect(run.heartbeat).toHaveBeenCalledTimes(1)
      await vi.advanceTimersByTimeAsync(600)
      expect(run.heartbeat).toHaveBeenCalledTimes(3)

      handler.resolve()
      await running
      expect(vi.getTimerCount()).toBe(0)
      await vi.advanceTimersByTimeAsync(900)
      expect(run.heartbeat).toHaveBeenCalledTimes(3)
    })

    it('aborts the signal and skips terminal writes after heartbeat lease loss', async () => {
      const events: JobEvent[] = []
      const queue = new JobQueue(makeBackend(), silentLogger, {
        visibilityTimeoutMs: 900,
        onJobEvent: (event) => events.push(event),
      })
      const run = makeHandle({ n: 1 })
      run.heartbeat.mockResolvedValue('lease-lost')
      let signal!: AbortSignal
      const running = queue.runClaimed(run.handle, async (_job, ctx) => {
        signal = ctx.signal
        await new Promise<void>((resolve) => {
          ctx.signal.addEventListener('abort', () => resolve(), { once: true })
        })
      })

      await vi.advanceTimersByTimeAsync(300)
      await running.catch(() => undefined)

      expect(signal.aborted).toBe(true)
      expect(run.complete).not.toHaveBeenCalled()
      expect(run.fail).not.toHaveBeenCalled()
      expect(run.failFatal).not.toHaveBeenCalled()
      expect(events).toContainEqual(
        expect.objectContaining({ kind: 'lease-lost', op: 'heartbeat' }),
      )
      expect(vi.getTimerCount()).toBe(0)
    })

    it('aborts when heartbeat renewal stays unconfirmed through the lease deadline', async () => {
      const events: JobEvent[] = []
      const queue = new JobQueue(makeBackend(), silentLogger, {
        visibilityTimeoutMs: 900,
        onJobEvent: (event) => events.push(event),
      })
      const run = makeHandle(
        { n: 1 },
        { claimedAt: new Date(Date.now() - 100) },
      )
      run.heartbeat.mockRejectedValue(new Error('heartbeat unavailable'))
      let signal!: AbortSignal
      const running = queue.runClaimed(run.handle, async (_job, ctx) => {
        signal = ctx.signal
        await new Promise<void>((resolve) => {
          ctx.signal.addEventListener('abort', () => resolve(), { once: true })
        })
      })

      await vi.advanceTimersByTimeAsync(799)
      expect(signal.aborted).toBe(false)
      await vi.advanceTimersByTimeAsync(1)
      await running

      expect(signal.aborted).toBe(true)
      expect(run.heartbeat).toHaveBeenCalledTimes(2)
      expect(run.complete).not.toHaveBeenCalled()
      expect(events).toContainEqual(
        expect.objectContaining({
          kind: 'lease-lost',
          op: 'heartbeat-deadline',
        }),
      )
      expect(vi.getTimerCount()).toBe(0)
    })
  })

  it('emits terminal events only after their durable write resolves', async () => {
    const events: JobEvent[] = []
    const queue = new JobQueue(makeBackend(), silentLogger, {
      onJobEvent: (event) => events.push(event),
    })
    const run = makeHandle({ n: 1 })
    const completion = deferred<CompleteJobResult>()
    run.complete.mockReturnValue(completion.promise)

    const running = queue.runClaimed(run.handle, async () => undefined)
    await Promise.resolve()
    expect(events.map((event) => event.kind)).toEqual(['claimed'])

    completion.resolve({ status: 'completed' })
    await running
    expect(events.map((event) => event.kind)).toEqual(['claimed', 'completed'])
  })
})

describe('JobQueue.runClaimed same-key draining', () => {
  it('defaults to ten additional same-key claims and leaves surplus unclaimed', async () => {
    const followers = Array.from({ length: 11 }, (_, index) =>
      makeHandle(index + 1, { id: `f${index + 1}` }),
    )
    const claimNextByKey = vi.fn(async () => followers.shift()?.handle ?? null)
    const queue = new JobQueue(
      makeBackend({
        claimNextByKey:
          claimNextByKey as unknown as IJobQueueBackend['claimNextByKey'],
      }),
      silentLogger,
    )
    const initial = makeHandle(0, { id: 'initial' })
    const seen: number[] = []

    await queue.runClaimed(initial.handle, async (job) => {
      seen.push(job.data as number)
    })

    expect(seen).toEqual(Array.from({ length: 11 }, (_, index) => index))
    expect(claimNextByKey).toHaveBeenCalledTimes(10)
    expect(claimNextByKey).toHaveBeenCalledWith('sync', 'account:1')
    expect(followers).toHaveLength(1)
    expect(followers[0]!.complete).not.toHaveBeenCalled()
  })

  it.each([
    [0, [0]],
    [2, [0, 1, 2]],
  ] as const)('honors maxDrains=%i', async (maxDrains, expected) => {
    const followers = [
      makeHandle(1, { id: 'f1' }),
      makeHandle(2, { id: 'f2' }),
      makeHandle(3, { id: 'f3' }),
    ]
    const claimNextByKey = vi.fn(async () => followers.shift()?.handle ?? null)
    const queue = new JobQueue(
      makeBackend({
        claimNextByKey:
          claimNextByKey as unknown as IJobQueueBackend['claimNextByKey'],
      }),
      silentLogger,
    )
    const initial = makeHandle(0, { id: 'initial' })
    const seen: number[] = []

    await queue.runClaimed(
      initial.handle,
      async (job) => {
        seen.push(job.data as number)
      },
      { maxDrains },
    )

    expect(seen).toEqual(expected)
    expect(claimNextByKey).toHaveBeenCalledTimes(maxDrains)
  })

  it('drains a newer follower after superseding a failed claim, then throws the original error', async () => {
    const events: JobEvent[] = []
    const follower = makeHandle(2, { id: 'follower' })
    const claimNextByKey = vi.fn().mockResolvedValueOnce(follower.handle)
    const queue = new JobQueue(
      makeBackend({
        claimNextByKey:
          claimNextByKey as unknown as IJobQueueBackend['claimNextByKey'],
      }),
      silentLogger,
      { onJobEvent: (event) => events.push(event) },
    )
    const initial = makeHandle(1, { id: 'initial' })
    initial.fail.mockResolvedValue({ status: 'superseded' })
    const original = new Error('stale payload failed')
    const seen: number[] = []

    await expect(
      queue.runClaimed(initial.handle, async (job) => {
        seen.push(job.data as number)
        if (job.id === initial.handle.id) throw original
      }),
    ).rejects.toBe(original)

    expect(seen).toEqual([1, 2])
    expect(initial.fail).toHaveBeenCalledWith('stale payload failed')
    expect(follower.complete).toHaveBeenCalledTimes(1)
    expect(events.map((event) => event.kind)).toEqual([
      'claimed',
      'superseded',
      'claimed',
      'completed',
    ])
  })

  it('uses the same superseded drain chain for queue.process', async () => {
    const follower = makeHandle(2, { id: 'follower' })
    const claimNext = vi
      .fn()
      .mockResolvedValueOnce(activeJob({ id: 'initial', data: 1 }))
      .mockResolvedValue(null)
    const claimNextByKey = vi.fn().mockResolvedValueOnce(follower.handle)
    const backend = makeBackend({
      claimNext,
      claimNextByKey:
        claimNextByKey as unknown as IJobQueueBackend['claimNextByKey'],
      fail: vi.fn().mockResolvedValue({ status: 'superseded' }),
    })
    const queue = new JobQueue(backend, silentLogger)
    const finished = deferred<void>()
    const seen: number[] = []
    queue.process('sync', (job) => {
      seen.push(job.data as number)
      if (job.id === 'initial') throw new Error('stale payload failed')
      finished.resolve()
    })

    await queue.enqueue('sync', {})
    await finished.promise
    await queue.shutdown()

    expect(seen).toEqual([1, 2])
    expect(claimNextByKey).toHaveBeenCalledWith('sync', 'account:1')
    expect(follower.complete).toHaveBeenCalledTimes(1)
  })

  it.each([-1, Number.POSITIVE_INFINITY, Number.NaN])(
    'rejects invalid maxDrains %s before running the handler',
    async (maxDrains) => {
      const queue = new JobQueue(makeBackend(), silentLogger)
      const initial = makeHandle(0)
      const handler = vi.fn()

      await expect(
        queue.runClaimed(initial.handle, handler, { maxDrains }),
      ).rejects.toThrow(/maxDrains/)
      expect(handler).not.toHaveBeenCalled()
      expect(initial.complete).not.toHaveBeenCalled()
    },
  )

  it('leaves the follower pending when the post-completion claim write crashes', async () => {
    const claimError = new Error('claim write unavailable')
    const claimNextByKey = vi.fn().mockRejectedValue(claimError)
    const queue = new JobQueue(makeBackend({ claimNextByKey }), silentLogger)
    const initial = makeHandle(0)

    await expect(
      queue.runClaimed(initial.handle, async () => undefined),
    ).rejects.toBe(claimError)

    expect(initial.complete).toHaveBeenCalledTimes(1)
    expect(claimNextByKey).toHaveBeenCalledTimes(1)
  })
})

describe('JobQueue managed shutdown', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('stops claims, waits through grace, then aborts and releases the claim', async () => {
    const events: JobEvent[] = []
    const release = vi.fn().mockResolvedValue({ status: 'released' })
    const claimNext = vi
      .fn()
      .mockResolvedValueOnce(activeJob())
      .mockResolvedValue(null)
    const backend = makeBackend({ claimNext, release })
    const queue = new JobQueue(backend, silentLogger, {
      visibilityTimeoutMs: 900,
      onJobEvent: (event) => events.push(event),
    })
    const started = deferred<void>()
    let signal!: AbortSignal
    queue.process('sync', async (_job, ctx) => {
      signal = ctx.signal
      started.resolve()
      await new Promise<void>((resolve) => {
        ctx.signal.addEventListener('abort', () => resolve(), { once: true })
      })
    })
    await queue.enqueue('sync', {})
    await started.promise

    const stopping = queue.shutdown(300)
    await vi.advanceTimersByTimeAsync(299)
    expect(signal.aborted).toBe(false)
    expect(release).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(1)
    await stopping
    expect(signal.aborted).toBe(true)
    expect(release).toHaveBeenCalledWith('j1', 'claim-1')
    expect(claimNext).toHaveBeenCalledTimes(1)
    expect(backend.complete).not.toHaveBeenCalled()
    expect(backend.fail).not.toHaveBeenCalled()
    expect(events).toContainEqual({
      kind: 'shutdown-released',
      type: 'sync',
      jobId: 'j1',
    })
  })

  it('accepts a superseded shutdown release without a second terminal write', async () => {
    const release = vi.fn().mockResolvedValue({ status: 'superseded' })
    const failFatal = vi.fn().mockResolvedValue({ status: 'failed-terminal' })
    const claimNext = vi
      .fn()
      .mockResolvedValueOnce(activeJob())
      .mockResolvedValue(null)
    const backend = makeBackend({ claimNext, release, failFatal })
    const queue = new JobQueue(backend, silentLogger)
    const started = deferred<void>()
    queue.process('sync', async (_job, ctx) => {
      started.resolve()
      await new Promise<void>((resolve) => {
        ctx.signal.addEventListener('abort', () => resolve(), { once: true })
      })
    })
    await queue.enqueue('sync', {})
    await started.promise

    const stopping = queue.shutdown(1)
    await vi.advanceTimersByTimeAsync(1)
    await stopping

    expect(release).toHaveBeenCalledWith('j1', 'claim-1')
    expect(failFatal).not.toHaveBeenCalled()
  })

  it('bounds a hung release and leaves the active job for the reaper', async () => {
    const backend = new DummyBackend()
    const release = vi
      .spyOn(backend, 'release')
      .mockImplementation(() => new Promise<never>(() => undefined))
    const complete = vi.spyOn(backend, 'complete')
    const fail = vi.spyOn(backend, 'fail')
    const queue = new JobQueue(backend, silentLogger, {
      visibilityTimeoutMs: 900,
    })
    const started = deferred<void>()
    let jobId!: string
    queue.process('sync', async (job, ctx) => {
      jobId = job.id
      started.resolve()
      await new Promise<void>((resolve) => {
        ctx.signal.addEventListener('abort', () => resolve(), { once: true })
      })
    })
    await queue.enqueue('sync', {})
    await started.promise

    let settled = false
    const stopping = queue.shutdown(100).then(() => {
      settled = true
    })
    await vi.runAllTimersAsync()
    await stopping

    expect(settled).toBe(true)
    expect(release).toHaveBeenCalledWith(jobId, expect.any(String))
    expect(complete).not.toHaveBeenCalled()
    expect(fail).not.toHaveBeenCalled()
    expect(backend.jobs.find((job) => job.id === jobId)?.status).toBe('active')
  })

  it('rejects claimOrEnqueue after shutdown before calling the backend', async () => {
    const backend = makeBackend()
    const queue = new JobQueue(backend, silentLogger)
    await queue.shutdown(0)

    await expect(queue.claimOrEnqueue('sync', {})).rejects.toThrow(
      /shutting down/,
    )
    expect(backend.claimOrEnqueue).not.toHaveBeenCalled()
  })

  it('releases a claim that returns after shutdown wins the admission race', async () => {
    const claimed = deferred<JobHandle<unknown> | null>()
    const run = makeHandle({})
    const backend = makeBackend({
      claimOrEnqueue: vi.fn().mockReturnValue(claimed.promise),
    })
    const queue = new JobQueue(backend, silentLogger)

    const claiming = queue.claimOrEnqueue('sync', {})
    const rejected = expect(claiming).rejects.toThrow(/shutting down/)
    await queue.shutdown(0)
    claimed.resolve(run.handle)
    await rejected

    expect(run.release).toHaveBeenCalledTimes(1)
  })

  it('releases an initial runClaimed handle when shutdown already started', async () => {
    const queue = new JobQueue(makeBackend(), silentLogger)
    const run = makeHandle({})
    const handler = vi.fn()
    await queue.shutdown(0)

    await expect(queue.runClaimed(run.handle, handler)).rejects.toThrow(
      /shutting down/,
    )

    expect(handler).not.toHaveBeenCalled()
    expect(run.release).toHaveBeenCalledTimes(1)
  })
})

describe('ImmediateBackend queue.process parity', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('awaits the managed callback and terminal write before enqueue resolves', async () => {
    const backend = new ImmediateBackend()
    const complete = vi.spyOn(backend, 'complete')
    const queue = new JobQueue(backend, silentLogger)
    const handler = deferred<void>()
    const callback = vi.fn(() => handler.promise)
    queue.process('inline', callback)
    let settled = false

    try {
      const enqueueing = queue.enqueue('inline', { n: 1 }).then((id) => {
        settled = true
        return id
      })
      await vi.advanceTimersByTimeAsync(0)

      expect(callback).toHaveBeenCalledTimes(1)
      expect(settled).toBe(false)
      expect(complete).not.toHaveBeenCalled()

      handler.resolve()
      const id = await enqueueing
      expect(complete).toHaveBeenCalledTimes(1)
      expect((await backend.findOne({ id }))?.status).toBe('completed')
    } finally {
      await queue.shutdown(0)
    }
  })

  it('enforces the configured concurrency for inline handlers', async () => {
    const backend = new ImmediateBackend()
    const queue = new JobQueue(backend, silentLogger)
    const firstStarted = deferred<void>()
    const releaseFirst = deferred<void>()
    let active = 0
    let maxActive = 0
    const callback = vi.fn(async (job: Job<{ n: number }>) => {
      active++
      maxActive = Math.max(maxActive, active)
      if (job.data.n === 1) {
        firstStarted.resolve()
        await releaseFirst.promise
      }
      active--
    })
    queue.process('inline', callback, { concurrency: 1 })

    try {
      const first = queue.enqueue('inline', { n: 1 })
      await firstStarted.promise
      const second = queue.enqueue('inline', { n: 2 })
      await vi.advanceTimersByTimeAsync(0)
      expect(callback).toHaveBeenCalledTimes(1)

      releaseFirst.resolve()
      await Promise.all([first, second])
      expect(callback).toHaveBeenCalledTimes(2)
      expect(maxActive).toBe(1)
    } finally {
      await queue.shutdown(0)
    }
  })
})
