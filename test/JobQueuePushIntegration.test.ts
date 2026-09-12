/**
 * Unit tests for JobQueue's push/poll interaction with a backend that
 * implements `onJobAvailable` but may or may not actually provide push
 * notifications (e.g. MongoJobQueue with change streams flag off).
 *
 * Regression coverage for the bug reported in PR #913 review where a
 * backend always exposing `onJobAvailable` caused JobQueue to switch to
 * the 60s safety-net poll interval even when change streams were
 * disabled — a 12x polling regression.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { JobQueue } from '../src/JobQueue'
import type { Job, JobHandle, QueueStats } from '../src/types'

/**
 * Peek at the internal processor map. JobQueue doesn't expose `pollInterval`
 * publicly, and the tests in this file are specifically locking in that
 * private invariant as a regression guard. One small helper keeps the
 * `(queue as unknown as …)` cast in one place so a future rename of
 * `processors` or `config.pollInterval` fails loudly from one line, not four.
 */
function getPollInterval(q: JobQueue, type: string): number | undefined {
  return (
    q as unknown as {
      processors: Map<string, { config: { pollInterval: number } }>
    }
  ).processors.get(type)?.config.pollInterval
}

function makeStubBackend(
  onJobAvailable:
    | ((listener: (type: string) => void) => (() => void) | null)
    | undefined,
): IJobQueueBackend {
  return {
    enqueue: vi.fn().mockResolvedValue('stub-id'),
    claimOrEnqueue: vi.fn().mockResolvedValue(null as JobHandle | null),
    claimNext: vi.fn().mockResolvedValue(null as Job | null),
    claimNextByKey: vi
      .fn()
      .mockResolvedValue(null) as unknown as IJobQueueBackend['claimNextByKey'],
    complete: vi.fn().mockResolvedValue({ status: 'completed' }),
    fail: vi.fn().mockResolvedValue({ status: 'retry-scheduled' }),
    failFatal: vi.fn().mockResolvedValue({ status: 'failed-terminal' }),
    release: vi.fn().mockResolvedValue({ status: 'released' }),
    log: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue('applied'),
    hasOutstanding: vi.fn().mockResolvedValue(false),
    listFailed: vi.fn().mockResolvedValue([] as Job[]),
    retry: vi.fn().mockResolvedValue(false),
    findOne: vi.fn().mockResolvedValue(null as Job | null),
    getStats: vi.fn().mockResolvedValue({
      pending: 0,
      active: 0,
      completed: 0,
      failed: 0,
    } as QueueStats),
    startup: vi.fn().mockResolvedValue(undefined),
    shutdown: vi.fn().mockResolvedValue(undefined),
    resetStorage: vi.fn().mockResolvedValue(undefined),
    onJobAvailable,
  }
}

describe('JobQueue push/poll selection', () => {
  let queue: JobQueue

  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(async () => {
    // Explicit shutdown before restoring real timers so the poll-loop
    // setTimeout inside JobQueue is cleared from the fake-timer queue and
    // can't leak into sibling test files running in the same Vitest worker.
    await queue?.shutdown()
    vi.useRealTimers()
  })

  it('uses the 5s default poll interval when backend has no onJobAvailable', () => {
    const backend = makeStubBackend(undefined)
    queue = new JobQueue(backend)
    queue.process('typeA', async () => undefined)
    expect(getPollInterval(queue, 'typeA')).toBe(5000)
  })

  it('uses the 5s default poll interval when onJobAvailable returns null (push disabled)', () => {
    // MongoJobQueue with change streams off returns null — JobQueue must
    // treat this as "no push" and NOT switch to the safety-net interval.
    const backend = makeStubBackend(() => null)
    queue = new JobQueue(backend)
    queue.process('typeB', async () => undefined)
    expect(getPollInterval(queue, 'typeB')).toBe(5000)
  })

  it('uses the 60s safety-net poll interval when onJobAvailable returns a live unsubscribe', () => {
    const backend = makeStubBackend(() => () => undefined)
    queue = new JobQueue(backend)
    queue.process('typeC', async () => undefined)
    expect(getPollInterval(queue, 'typeC')).toBe(60000)
  })

  it('respects explicit pollInterval override regardless of push state', () => {
    const backend = makeStubBackend(() => () => undefined)
    queue = new JobQueue(backend)
    queue.process('typeD', async () => undefined, { pollInterval: 1234 })
    expect(getPollInterval(queue, 'typeD')).toBe(1234)
  })

  it('fans out the empty-string catch-up sentinel to every registered processor', async () => {
    // After a change stream reconnect, MongoChangeStreamWatcher dispatches
    // an empty-string type as a catch-up signal. JobQueue must translate
    // that into a claimNext() probe for every processor so jobs that
    // landed during the reconnect gap get picked up without waiting for
    // the 60s safety-net poll.
    let pushListener: ((type: string) => void) | undefined
    const backend = makeStubBackend((listener) => {
      pushListener = listener
      return () => undefined
    })
    queue = new JobQueue(backend)
    queue.process('typeE', async () => undefined)
    queue.process('typeF', async () => undefined)

    // Sanity: initial processor-loop poll already called claimNext once
    // per type. Reset so we can count the catch-up call in isolation.
    const claimNextRef = backend.claimNext
    const claimNext = claimNextRef as unknown as ReturnType<typeof vi.fn>
    claimNext.mockClear()

    expect(pushListener).toBeDefined()
    pushListener!('') // catch-up sentinel

    // tryProcessNext fires claimAndProcess synchronously (void-returning),
    // but the actual backend.claimNext call is awaited inside. Flush the
    // microtask queue so the mock gets hit before we assert.
    await Promise.resolve()

    const calledTypes = claimNext.mock.calls.map((c) => c[0])
    expect(calledTypes).toContain('typeE')
    expect(calledTypes).toContain('typeF')
  })

  describe('startup failure', () => {
    // A backend that advertises push (a live unsubscribe from onJobAvailable)
    // buffers the listener pre-startup. If backend.startup() then rejects
    // (e.g. change streams on a non-replica-set mongod), the queue must not
    // stay parked on the 60s safety-net interval with nothing left to wake
    // it — pickup latency was silently 60s after a one-line startup warning.

    it('returns processors to the default poll interval when startup() rejects', async () => {
      const unsubscribe = vi.fn()
      const backend = makeStubBackend(() => unsubscribe)
      backend.startup = vi.fn().mockRejectedValue(new Error('not a replica set'))
      queue = new JobQueue(backend)
      queue.process('a', async () => undefined)

      expect(getPollInterval(queue, 'a')).toBe(60000)

      await expect(queue.startup()).rejects.toThrow('not a replica set')

      expect(getPollInterval(queue, 'a')).toBe(5000)
      expect(unsubscribe).toHaveBeenCalledOnce()
    })

    it('leaves an explicit pollInterval alone on startup failure', async () => {
      const backend = makeStubBackend(() => vi.fn())
      backend.startup = vi.fn().mockRejectedValue(new Error('not a replica set'))
      queue = new JobQueue(backend)
      queue.process('b', async () => undefined, { pollInterval: 250 })

      await expect(queue.startup()).rejects.toThrow('not a replica set')

      expect(getPollInterval(queue, 'b')).toBe(250)
    })

    it('gives processors registered after a failed startup the default interval', async () => {
      const backend = makeStubBackend(() => vi.fn())
      backend.startup = vi.fn().mockRejectedValue(new Error('not a replica set'))
      queue = new JobQueue(backend)

      await expect(queue.startup()).rejects.toThrow('not a replica set')

      queue.process('c', async () => undefined)
      expect(getPollInterval(queue, 'c')).toBe(5000)
    })

    it('wakes a parked poll loop so the new interval applies immediately', async () => {
      const backend = makeStubBackend(() => vi.fn())
      backend.startup = vi.fn().mockRejectedValue(new Error('not a replica set'))
      queue = new JobQueue(backend)
      queue.process('d', async () => undefined)

      const claimNext = backend.claimNext as unknown as ReturnType<typeof vi.fn>
      claimNext.mockClear()

      await expect(queue.startup()).rejects.toThrow('not a replica set')

      // On the old 60s interval this loop wouldn't wake for another 55s.
      await vi.advanceTimersByTimeAsync(5000)

      expect(claimNext).toHaveBeenCalled()
    })
  })
})
