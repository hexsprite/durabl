import { describe, expect, it } from 'vitest'

import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { JobQueue } from '../src/JobQueue'
import type { Job, JobHandle, JobStatus, QueueStats } from '../src/types'

/**
 * Minimal push-capable backend used to exercise the JobQueue concurrency
 * gate deterministically. `claimNext` pops one pending job of the type with a
 * real microtask yield, so two concurrent claims interleave at the `await` —
 * exactly the change-stream-burst scenario.
 */
class MiniBackend implements IJobQueueBackend {
  private pending: Job[] = []
  private listener: ((type: string) => void) | null = null
  private seq = 0

  seed(type: string, count: number): void {
    for (let i = 0; i < count; i++) {
      this.pending.push({
        id: `job-${this.seq++}`,
        type,
        data: {},
        status: 'pending',
        attempt: 0,
        maxAttempts: 3,
        priority: 0,
        runAt: new Date(),
        createdAt: new Date(),
      })
    }
  }

  /** Fire the push listener as a change-stream notification would. */
  notify(type: string): void {
    this.listener?.(type)
  }

  async enqueue(): Promise<string | null> {
    return 'x'
  }
  async claimOrEnqueue<T>(): Promise<JobHandle<T> | null> {
    return null
  }
  async claimNext<T>(type: string): Promise<Job<T> | null> {
    // Yield once so concurrent callers interleave across the await — the
    // window the TOCTOU lives in.
    await Promise.resolve()
    const idx = this.pending.findIndex((j) => j.type === type)
    if (idx === -1) return null
    const [job] = this.pending.splice(idx, 1)
    job.status = 'active' as JobStatus
    job.attempt++
    return job as Job<T>
  }
  async complete(): Promise<void> {}
  async fail(): Promise<void> {}
  async failFatal(): Promise<void> {}
  async log(): Promise<void> {}
  async heartbeat(): Promise<void> {}
  async findOne<T>(): Promise<Job<T> | null> {
    return null
  }
  async getStats(): Promise<QueueStats> {
    return { pending: this.pending.length, active: 0, completed: 0, failed: 0 }
  }
  async startup(): Promise<void> {}
  async shutdown(): Promise<void> {}
  async resetStorage(): Promise<void> {}
  onJobAvailable(listener: (type: string) => void): () => void {
    this.listener = listener
    return () => {
      this.listener = null
    }
  }
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

describe('JobQueue concurrency cap', () => {
  // Regression (du-12z): tryProcessNext/claimAndProcess checked
  // `activeCount < concurrency`, then `await claimNext`, then incremented.
  // A burst of push notifications (change-stream reconnect / multiple inserts)
  // drives N concurrent claimAndProcess calls that all pass the check before
  // any increments, each claiming a distinct job — overshooting the cap on a
  // single instance. The atomic per-job claim does NOT protect against this.
  it('never runs more than `concurrency` jobs at once under a push burst', async () => {
    const backend = new MiniBackend()
    const queue = new JobQueue(backend)

    backend.seed('t', 5)

    let active = 0
    let maxObserved = 0
    let completed = 0
    queue.process(
      't',
      async () => {
        active++
        maxObserved = Math.max(maxObserved, active)
        await sleep(20)
        active--
        completed++
      },
      { concurrency: 2, pollInterval: 100000 },
    )

    // Fire the push listener several times in the same tick — the burst.
    for (let i = 0; i < 5; i++) backend.notify('t')

    // Let everything drain.
    while (completed < 5) await sleep(10)
    await queue.shutdown(1000)

    expect(maxObserved).toBeLessThanOrEqual(2)
  })

  it('respects concurrency=1 under a burst', async () => {
    const backend = new MiniBackend()
    const queue = new JobQueue(backend)

    backend.seed('t', 4)

    let active = 0
    let maxObserved = 0
    let completed = 0
    queue.process(
      't',
      async () => {
        active++
        maxObserved = Math.max(maxObserved, active)
        await sleep(15)
        active--
        completed++
      },
      { concurrency: 1, pollInterval: 100000 },
    )

    for (let i = 0; i < 4; i++) backend.notify('t')

    while (completed < 4) await sleep(10)
    await queue.shutdown(1000)

    expect(maxObserved).toBe(1)
  })
})
