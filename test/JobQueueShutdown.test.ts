/**
 * Regression: `shutdown()` resolved while the poll loops were still parked in
 * a plain `setTimeout`. Nothing cleared those timers, so they kept the event
 * loop alive for up to one `pollInterval` after a "graceful" shutdown — 60s
 * with change streams active. A service that awaited shutdown and expected to
 * exit instead hung.
 */
import { describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { JobQueue } from '../src/JobQueue'

import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

/** Timers currently holding the event loop open. */
function timerCount(): number {
  return process
    .getActiveResourcesInfo()
    .filter((r) => r === 'Timeout').length
}

describe('JobQueue.shutdown()', () => {
  it('leaves no armed poll timer behind', async () => {
    const before = timerCount()

    const queue = new JobQueue(new DummyBackend(), silentLogger)
    // Long enough that a leaked timer is unmistakable rather than a race.
    queue.process('slowpoll', async () => {}, {
      pollInterval: 300000,
    })
    await waitUntil(() => timerCount() > before)

    await queue.shutdown(1000)

    expect(timerCount()).toBe(before)
  })

  it('returns promptly instead of waiting out the poll interval', async () => {
    const queue = new JobQueue(new DummyBackend(), silentLogger)
    queue.process('slowpoll', async () => {}, {
      pollInterval: 300000,
    })
    await waitUntil(() => timerCount() > 0)

    const started = Date.now()
    await queue.shutdown(1000)

    expect(Date.now() - started).toBeLessThan(1000)
  })

  it('cancels the sleeps of every registered processor, not just one', async () => {
    const before = timerCount()

    const queue = new JobQueue(new DummyBackend(), silentLogger)
    for (const type of ['a', 'b', 'c']) {
      queue.process(type, async () => {}, {
        pollInterval: 300000,
      })
    }
    await waitUntil(() => timerCount() >= before + 3)

    await queue.shutdown(1000)

    expect(timerCount()).toBe(before)
  })

  it('still drains an in-flight job before resolving', async () => {
    const backend = new DummyBackend()
    const queue = new JobQueue(backend, silentLogger)

    let started = false
    let finished = false
    let release!: () => void
    const parked = new Promise<void>((resolve) => {
      release = resolve
    })

    queue.process('drainme', async () => {
      started = true
      await parked
      finished = true
    })
    await queue.enqueue('drainme', {})
    await waitUntil(() => started)

    const shutdown = queue.shutdown(5000)
    // Shutdown must not resolve while the handler is still running.
    await new Promise((r) => setTimeout(r, 50))
    expect(finished).toBe(false)

    release()
    await shutdown
    expect(finished).toBe(true)
  })

  it('is safe to call twice', async () => {
    const queue = new JobQueue(new DummyBackend(), silentLogger)
    queue.process('slowpoll', async () => {}, {
      pollInterval: 300000,
    })

    await queue.shutdown(1000)
    await expect(queue.shutdown(1000)).resolves.toBeUndefined()
  })
})
