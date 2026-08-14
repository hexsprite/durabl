/**
 * Reaper wiring (du-jt0): the queue's `visibilityTimeoutMs` is the single
 * source of truth for the lease window. Before `startReaper`, operators
 * invoked `backend.recoverStuckJobs(visibilityTimeoutMs)` with an independent
 * parameter that agreed with the queue's value only by coincident defaults —
 * a drifted value reaped jobs out from under live workers.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import { JobQueue } from '../src/JobQueue'
import type { Logger } from '../src/logger'
import type { JobEvent } from '../src/types'
import { silentLogger } from './testLogger'

let backend: DummyBackend
let queue: JobQueue

function deferred<T>(): {
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

beforeEach(() => {
  vi.useFakeTimers()
  backend = new DummyBackend()
  queue = new JobQueue(backend, silentLogger, { visibilityTimeoutMs: 12345 })
})

afterEach(async () => {
  vi.useRealTimers()
  await queue.shutdown(100)
})

describe('JobQueue.startReaper (du-jt0, du-zcz.4)', () => {
  it('runs an immediate sweep with the queue visibility timeout before arming the timer', async () => {
    backend.recoverStuckJobs = async (visibilityTimeoutMs?: number) => {
      backend.recoverStuckJobsCalls.push(visibilityTimeoutMs)
      return 7
    }

    const starting = queue.startReaper(1000)

    expect(backend.recoverStuckJobsCalls).toEqual([12345])
    expect(vi.getTimerCount()).toBe(0)
    await expect(starting).resolves.toEqual({
      status: 'started',
      recovered: 7,
    })
    expect(vi.getTimerCount()).toBe(1)
  })

  it('returns recovered null after a failed startup sweep, reports the error, and keeps running', async () => {
    const events: JobEvent[] = []
    const error = vi.fn()
    const logger: Logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error,
      child: () => logger,
    }
    backend.recoverStuckJobs = vi
      .fn()
      .mockRejectedValueOnce(new Error('startup broke'))
      .mockResolvedValueOnce(2)
    queue = new JobQueue(backend, logger, {
      visibilityTimeoutMs: 12345,
      onJobEvent: (event) => events.push(event),
    })

    await expect(queue.startReaper(1000)).resolves.toEqual({
      status: 'started',
      recovered: null,
    })

    expect(error).toHaveBeenCalledTimes(1)
    expect(events).toEqual([
      {
        kind: 'reaper-error',
        phase: 'startup',
        message: 'startup broke',
      },
    ])
    expect(vi.getTimerCount()).toBe(1)

    await vi.advanceTimersByTimeAsync(1000)

    expect(backend.recoverStuckJobs).toHaveBeenCalledTimes(2)
    expect(events[1]).toEqual({
      kind: 'reaper-recovered',
      handled: 2,
      saturated: false,
    })
  })

  it('reports periodic errors separately and retries on the next interval', async () => {
    const events: JobEvent[] = []
    backend.recoverStuckJobs = vi
      .fn()
      .mockResolvedValueOnce(0)
      .mockRejectedValueOnce(new Error('periodic broke'))
      .mockResolvedValueOnce(0)
    queue = new JobQueue(backend, silentLogger, {
      visibilityTimeoutMs: 12345,
      onJobEvent: (event) => events.push(event),
    })
    await queue.startReaper(1000)

    await vi.advanceTimersByTimeAsync(1000)

    expect(events).toEqual([
      {
        kind: 'reaper-error',
        phase: 'periodic',
        message: 'periodic broke',
      },
    ])
    expect(vi.getTimerCount()).toBe(1)

    await vi.advanceTimersByTimeAsync(1000)
    expect(backend.recoverStuckJobs).toHaveBeenCalledTimes(3)
  })

  it('rejects an unsupported backend without sweeping or arming a timer', async () => {
    const unsupportedQueue = new JobQueue(
      new ImmediateBackend(),
      silentLogger,
    )

    await expect(unsupportedQueue.startReaper()).rejects.toThrow(
      /recoverStuckJobs/,
    )
    expect(vi.getTimerCount()).toBe(0)
  })

  it('shares one startup promise and result between concurrent callers', async () => {
    const sweep = deferred<number>()
    backend.recoverStuckJobs = vi.fn(() => sweep.promise)

    const first = queue.startReaper(1000)
    const second = queue.startReaper(2000)

    expect(second).toBe(first)
    expect(backend.recoverStuckJobs).toHaveBeenCalledTimes(1)

    sweep.resolve(4)
    await expect(first).resolves.toEqual({
      status: 'started',
      recovered: 4,
    })
    await expect(second).resolves.toEqual({
      status: 'started',
      recovered: 4,
    })
    expect(vi.getTimerCount()).toBe(1)
  })

  it('reports already-running after startup has settled', async () => {
    await expect(queue.startReaper(1000)).resolves.toEqual({
      status: 'started',
      recovered: 0,
    })

    await expect(queue.startReaper(2000)).resolves.toEqual({
      status: 'already-running',
    })
    expect(backend.recoverStuckJobsCalls).toEqual([12345])
    expect(vi.getTimerCount()).toBe(1)
  })

  it('does not arm a timer when shutdown happens during the startup sweep', async () => {
    const sweep = deferred<number>()
    backend.recoverStuckJobs = vi.fn(() => sweep.promise)

    const starting = queue.startReaper(1000)
    const shuttingDown = queue.shutdown(100)
    sweep.resolve(0)
    await Promise.all([starting, shuttingDown])

    expect(backend.recoverStuckJobs).toHaveBeenCalledTimes(1)
    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(5000)
    expect(backend.recoverStuckJobs).toHaveBeenCalledTimes(1)
  })

  it('never overlaps the immediate sweep with periodic sweeps', async () => {
    const startupSweep = deferred<number>()
    const periodicSweep = deferred<number>()
    let call = 0
    backend.recoverStuckJobs = async (visibilityTimeoutMs?: number) => {
      backend.recoverStuckJobsCalls.push(visibilityTimeoutMs)
      call += 1
      if (call === 1) return startupSweep.promise
      if (call === 2) return periodicSweep.promise
      return 0
    }

    const starting = queue.startReaper(1000)
    await vi.advanceTimersByTimeAsync(10000)
    expect(backend.recoverStuckJobsCalls).toEqual([12345])

    startupSweep.resolve(0)
    await starting
    await vi.advanceTimersByTimeAsync(1000)
    expect(backend.recoverStuckJobsCalls).toEqual([12345, 12345])

    await vi.advanceTimersByTimeAsync(10000)
    expect(backend.recoverStuckJobsCalls).toEqual([12345, 12345])

    periodicSweep.resolve(0)
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(1000)
    expect(backend.recoverStuckJobsCalls).toEqual([12345, 12345, 12345])
  })

  it('emits normal and saturated recovery events for successful sweeps', async () => {
    const events: JobEvent[] = []
    backend.recoverStuckJobs = vi
      .fn()
      .mockResolvedValueOnce(3)
      .mockResolvedValueOnce(1000)
    queue = new JobQueue(backend, silentLogger, {
      visibilityTimeoutMs: 12345,
      onJobEvent: (event) => events.push(event),
    })

    await queue.startReaper(1000)
    await vi.advanceTimersByTimeAsync(1000)

    expect(events).toEqual([
      {
        kind: 'reaper-recovered',
        handled: 3,
        saturated: false,
      },
      {
        kind: 'reaper-recovered',
        handled: 1000,
        saturated: true,
      },
    ])
  })

  it('stopReaper() stops periodic sweeps and is safe to call twice', async () => {
    await queue.startReaper(1000)
    expect(backend.recoverStuckJobsCalls).toEqual([12345])

    queue.stopReaper()
    queue.stopReaper()
    await vi.advanceTimersByTimeAsync(5000)

    expect(backend.recoverStuckJobsCalls).toEqual([12345])
  })

  it('rejects a non-positive sweep interval', async () => {
    await expect(queue.startReaper(0)).rejects.toThrow(/intervalMs/)
    await expect(queue.startReaper(-1)).rejects.toThrow(/intervalMs/)
  })
})

describe('JobQueue visibilityTimeoutMs validation', () => {
  // Regression companion to du-e1s: a queue constructed with
  // visibilityTimeoutMs: 0 would have the reaper reclaim every active job
  // instantly and the Orchestrator heartbeat on setTimeout(0).
  it('rejects a zero or negative visibilityTimeoutMs at construction', () => {
    expect(
      () => new JobQueue(backend, silentLogger, { visibilityTimeoutMs: 0 }),
    ).toThrow(/visibilityTimeoutMs/)
    expect(
      () => new JobQueue(backend, silentLogger, { visibilityTimeoutMs: -5 }),
    ).toThrow(/visibilityTimeoutMs/)
  })

  it('rejects a non-finite visibilityTimeoutMs at construction', () => {
    expect(
      () => new JobQueue(backend, silentLogger, { visibilityTimeoutMs: NaN }),
    ).toThrow(/visibilityTimeoutMs/)
  })
})
