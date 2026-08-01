/**
 * Regression: the reaper was driven by `setInterval` and fired its sweep with
 * `void`, never awaiting the previous one. A sweep that outlasted the interval
 * — precisely what happens after a mass worker death, when the stuck set is
 * largest — overlapped with the next, multiplying cursor and write work at the
 * worst possible moment.
 *
 * Real timers, deliberately: the property under test is "the next tick is armed
 * only after the previous sweep settles", and fake timers would let a sweep's
 * awaits and the clock advance independently — which is the very interleaving
 * the bug lived in.
 */
import { afterEach, describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { JobQueue } from '../src/JobQueue'

import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

/** A backend whose sweeps take `durationMs` and record their own overlap. */
function slowSweepBackend(durationMs: number): {
  backend: DummyBackend
  calls: () => number
  maxConcurrent: () => number
} {
  const backend = new DummyBackend()
  let inFlight = 0
  let maxConcurrent = 0
  let calls = 0

  backend.recoverStuckJobs = async () => {
    calls++
    inFlight++
    maxConcurrent = Math.max(maxConcurrent, inFlight)
    await new Promise((r) => setTimeout(r, durationMs))
    inFlight--
    return 0
  }

  return {
    backend,
    calls: () => calls,
    maxConcurrent: () => maxConcurrent,
  }
}

describe('JobQueue.startReaper overlap', () => {
  const queues: JobQueue[] = []

  afterEach(async () => {
    await Promise.all(queues.splice(0).map((q) => q.shutdown(500)))
  })

  it('never runs two sweeps at once, even when a sweep outlasts the interval', async () => {
    const { backend, calls, maxConcurrent } = slowSweepBackend(80)
    const queue = new JobQueue(backend, silentLogger)
    queues.push(queue)

    queue.startReaper(10) // interval far shorter than the sweep
    await waitUntil(() => calls() >= 3, { timeoutMs: 5000 })
    queue.stopReaper()

    expect(maxConcurrent()).toBe(1)
  })

  it('is idempotent when start is called again mid-sweep', async () => {
    const { backend, calls, maxConcurrent } = slowSweepBackend(80)
    const queue = new JobQueue(backend, silentLogger)
    queues.push(queue)

    queue.startReaper(10)
    await waitUntil(() => calls() >= 1)
    queue.startReaper(10) // during an in-flight sweep: must not start a 2nd loop
    await waitUntil(() => calls() >= 3, { timeoutMs: 5000 })
    queue.stopReaper()

    expect(maxConcurrent()).toBe(1)
  })

  it('does not re-arm when stopped mid-sweep', async () => {
    const { backend, calls } = slowSweepBackend(60)
    const queue = new JobQueue(backend, silentLogger)
    queues.push(queue)

    queue.startReaper(10)
    await waitUntil(() => calls() >= 1)
    queue.stopReaper() // while the first sweep is still running

    const atStop = calls()
    await new Promise((r) => setTimeout(r, 200)) // several intervals
    expect(calls()).toBe(atStop)
  })

  it('does not re-arm after shutdown', async () => {
    const { backend, calls } = slowSweepBackend(20)
    const queue = new JobQueue(backend, silentLogger)

    queue.startReaper(10)
    await waitUntil(() => calls() >= 1)
    await queue.shutdown(500)

    const atShutdown = calls()
    await new Promise((r) => setTimeout(r, 150))
    expect(calls()).toBe(atShutdown)
  })

  it('keeps sweeping after a sweep throws', async () => {
    const backend = new DummyBackend()
    let calls = 0
    backend.recoverStuckJobs = async () => {
      calls++
      if (calls === 1) throw new Error('transient mongo failure')
      return 0
    }
    const queue = new JobQueue(backend, silentLogger)
    queues.push(queue)

    queue.startReaper(10)
    await waitUntil(() => calls >= 3, { timeoutMs: 5000 })
    queue.stopReaper()

    expect(calls).toBeGreaterThanOrEqual(3)
  })
})
