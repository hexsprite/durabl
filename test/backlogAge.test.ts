/**
 * Backlog age (du-8wi) — the metric worth alerting on.
 *
 * Depth cannot distinguish a healthy queue from a stuck one: 5 jobs waiting 40
 * minutes is an incident, 5000 draining fast is fine. These tests pin the two
 * properties that make the metric trustworthy: future-dated jobs are excluded
 * (or it pegs permanently red and gets ignored), and only pending jobs count.
 *
 * Covered here at the pure-function and in-memory-backend level; the Mongo
 * implementation and its query plan are covered in MongoJobQueue.test.ts.
 */
import { describe, expect, it, vi } from 'vitest'

import { backlogAge } from '../src/backends/backlogAge'
import { DummyBackend } from '../src/backends/DummyBackend'

const at = (offsetMs: number) => new Date(Date.now() + offsetMs)

describe('backlogAge', () => {
  it('reports null and zero for an empty list', () => {
    expect(backlogAge([])).toEqual({
      oldestPendingRunAt: null,
      oldestPendingLagMs: 0,
    })
  })

  it('measures lag from the oldest due pending job', () => {
    const now = new Date()
    const result = backlogAge(
      [
        { status: 'pending', runAt: new Date(now.getTime() - 30_000) },
        { status: 'pending', runAt: new Date(now.getTime() - 90_000) },
        { status: 'pending', runAt: new Date(now.getTime() - 10_000) },
      ],
      now,
    )
    expect(result.oldestPendingLagMs).toBe(90_000)
  })

  it('excludes jobs scheduled for the future', () => {
    const now = new Date()
    const result = backlogAge(
      [{ status: 'pending', runAt: new Date(now.getTime() + 3_600_000) }],
      now,
    )
    // A job deliberately delayed is scheduled, not late.
    expect(result.oldestPendingRunAt).toBeNull()
    expect(result.oldestPendingLagMs).toBe(0)
  })

  it('ignores non-pending jobs', () => {
    const now = new Date()
    const old = new Date(now.getTime() - 600_000)
    const result = backlogAge(
      [
        { status: 'active', runAt: old },
        { status: 'completed', runAt: old },
        { status: 'failed', runAt: old },
      ],
      now,
    )
    expect(result.oldestPendingRunAt).toBeNull()
  })

  it('never returns a negative lag', () => {
    const now = new Date()
    // Clock skew: runAt marginally in the future but treated as due elsewhere.
    const result = backlogAge(
      [{ status: 'pending', runAt: now }],
      new Date(now.getTime() - 5),
    )
    expect(result.oldestPendingLagMs).toBeGreaterThanOrEqual(0)
  })
})

describe('DummyBackend getStats: backlog age', () => {
  it('reports lag for an overdue pending job', async () => {
    const backend = new DummyBackend()
    await backend.enqueue('sync', {})
    const stats = await backend.getStats('sync')
    expect(stats.pending).toBe(1)
    expect(stats.oldestPendingLagMs).toBeGreaterThanOrEqual(0)
    expect(stats.oldestPendingRunAt).not.toBeNull()
  })

  it('counts a delayed job as pending but not as backlog', async () => {
    const backend = new DummyBackend()
    await backend.enqueue('sync', {}, { delay: 3_600_000 })
    const stats = await backend.getStats('sync')
    // Both halves matter: it IS queued, it is NOT late.
    expect(stats.pending).toBe(1)
    expect(stats.oldestPendingRunAt).toBeNull()
    expect(stats.oldestPendingLagMs).toBe(0)
  })

  it('reports zero for an empty queue', async () => {
    const backend = new DummyBackend()
    const stats = await backend.getStats('sync')
    expect(stats.oldestPendingRunAt).toBeNull()
    expect(stats.oldestPendingLagMs).toBe(0)
  })

  it('excludes an inline-claimed job from backlog', async () => {
    const backend = new DummyBackend()
    await backend.claimOrEnqueue('sync', {})
    const stats = await backend.getStats('sync')
    expect(stats.active).toBe(1)
    expect(stats.oldestPendingRunAt).toBeNull()
  })

  it('scopes backlog age to the requested type', async () => {
    const backend = new DummyBackend()
    await backend.enqueue('other', {})
    const stats = await backend.getStats('sync')
    expect(stats.oldestPendingRunAt).toBeNull()
  })
})

it('records a runAt honouring delay on the DummyBackend job', async () => {
  const backend = new DummyBackend()
  await backend.enqueue('sync', {}, { delay: 60_000 })
  // Read the stored job rather than claiming it: a delayed job is correctly
  // unclaimable now (see the runAt-on-claim suite below), so the claim result is
  // no longer a way to observe this. RecordedJob previously had no runAt at all
  // and claimNext substituted createdAt, which made a delayed job
  // indistinguishable from a due one to any caller.
  const [job] = backend.getJobsByType('sync')
  expect(job?.runAt.getTime()).toBeGreaterThan(at(30_000).getTime())
})

describe('in-memory backends honour runAt on claim (du-cir)', () => {
  /**
   * DummyBackend used to filter only on {type, status: 'pending'}, while Mongo
   * filters {type, status: 'pending', runAt: {$lte: now}}. So a delayed job was
   * claimable instantly here and correctly withheld in production — meaning a
   * unit test written against this backend could pass while the same logic
   * misbehaved for real. The whole point of the interface is that job logic is
   * testable without Mongo, and that only holds if the in-memory backends model
   * the same semantics.
   */
  it('does not claim a job that is not yet due', async () => {
    const backend = new DummyBackend()
    await backend.enqueue('sync', { n: 1 }, { delay: 60_000 })

    expect(await backend.claimNext('sync')).toBeNull()
  })

  it('claims the job once its runAt has passed', async () => {
    vi.useFakeTimers()
    try {
      const backend = new DummyBackend()
      await backend.enqueue('sync', { n: 1 }, { delay: 60_000 })
      expect(await backend.claimNext('sync')).toBeNull()

      vi.advanceTimersByTime(60_001)
      const claimed = await backend.claimNext<{ n: number }>('sync')
      expect(claimed?.data.n).toBe(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('skips a delayed job to claim a due one behind it', async () => {
    const backend = new DummyBackend()
    await backend.enqueue('sync', { late: true }, { delay: 60_000 })
    await backend.enqueue('sync', { due: true })

    const claimed = await backend.claimNext<{ due?: boolean }>('sync')
    // A future-dated job must not block work that is ready now.
    expect(claimed?.data.due).toBe(true)
  })

  it('claims undelayed jobs immediately, as before', async () => {
    const backend = new DummyBackend()
    await backend.enqueue('sync', { n: 1 })
    expect(await backend.claimNext('sync')).not.toBeNull()
  })
})
