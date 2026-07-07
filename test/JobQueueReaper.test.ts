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
import { silentLogger } from './testLogger'

let backend: DummyBackend
let queue: JobQueue

beforeEach(() => {
  vi.useFakeTimers()
  backend = new DummyBackend()
  queue = new JobQueue(backend, silentLogger, { visibilityTimeoutMs: 12345 })
})

afterEach(async () => {
  vi.useRealTimers()
  await queue.shutdown(100)
})

describe('JobQueue.startReaper (du-jt0)', () => {
  // Regression: recoverStuckJobs was operator-invoked with its own
  // visibilityTimeoutMs param (default 300000) that only matched the queue's
  // configured value by coincidence. The reaper must sweep with the queue's
  // value — the same one the Orchestrator sizes heartbeats from.
  it('sweeps with the queue-configured visibilityTimeoutMs, not an independent default', async () => {
    queue.startReaper(1000)

    await vi.advanceTimersByTimeAsync(3000)

    expect(backend.recoverStuckJobsCalls).toEqual([12345, 12345, 12345])
  })

  it('shutdown() stops the reaper so no sweeps fire afterwards', async () => {
    queue.startReaper(1000)
    await vi.advanceTimersByTimeAsync(1000)
    expect(backend.recoverStuckJobsCalls).toHaveLength(1)

    await queue.shutdown(100)
    await vi.advanceTimersByTimeAsync(5000)

    expect(backend.recoverStuckJobsCalls).toHaveLength(1)
  })

  it('stopReaper() stops sweeps and is safe to call twice', async () => {
    queue.startReaper(1000)
    await vi.advanceTimersByTimeAsync(1000)

    queue.stopReaper()
    queue.stopReaper()
    await vi.advanceTimersByTimeAsync(5000)

    expect(backend.recoverStuckJobsCalls).toHaveLength(1)
  })

  it('is idempotent: a second startReaper() does not double the sweeps', async () => {
    queue.startReaper(1000)
    queue.startReaper(1000)

    await vi.advanceTimersByTimeAsync(1000)

    expect(backend.recoverStuckJobsCalls).toHaveLength(1)
  })

  it('keeps sweeping after a failed sweep (transient backend error)', async () => {
    backend.recoverStuckJobs = async (visibilityTimeoutMs?: number) => {
      backend.recoverStuckJobsCalls.push(visibilityTimeoutMs)
      if (backend.recoverStuckJobsCalls.length === 1) {
        throw new Error('transient')
      }
      return 0
    }
    queue.startReaper(1000)

    await vi.advanceTimersByTimeAsync(2000)

    expect(backend.recoverStuckJobsCalls).toEqual([12345, 12345])
  })

  it('throws when the backend does not implement recoverStuckJobs', () => {
    const q = new JobQueue(new ImmediateBackend(), silentLogger)
    expect(() => q.startReaper()).toThrow(/recoverStuckJobs/)
  })

  it('rejects a non-positive sweep interval', () => {
    expect(() => queue.startReaper(0)).toThrow(/intervalMs/)
    expect(() => queue.startReaper(-1)).toThrow(/intervalMs/)
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
