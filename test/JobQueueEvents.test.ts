/**
 * onJobEvent metrics sink (du-id4).
 *
 * durabl injected a Logger but offered no way to get a counter or a histogram
 * out of it, so a consumer wanting throughput or failure-rate dashboards had to
 * parse structured logs and match on durabl's internal message strings — which
 * quietly made those strings a public API that any refactor breaks.
 *
 * Logs are for a human reading an incident; events are for a machine counting.
 */
import { describe, expect, it, vi } from 'vitest'

import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { JobQueue } from '../src/JobQueue'
import {
  FatalJobError,
  type Job,
  type JobEvent,
  type JobHandle,
  type QueueStats,
} from '../src/types'

const silent = {
  child: () => silent,
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
}

const aJob = (over: Partial<Job> = {}): Job => ({
  id: 'j1',
  type: 'sync',
  data: {},
  status: 'active',
  attempt: 1,
  maxAttempts: 3,
  priority: 0,
  runAt: new Date(),
  createdAt: new Date(),
  claimToken: 'tok',
  ...over,
})

function makeBackend(over: Partial<IJobQueueBackend> = {}): IJobQueueBackend {
  return {
    enqueue: vi.fn().mockResolvedValue('id'),
    claimOrEnqueue: vi.fn().mockResolvedValue(null as JobHandle | null),
    claimNext: vi.fn().mockResolvedValue(null as Job | null),
    complete: vi.fn().mockResolvedValue({ status: 'completed' }),
    fail: vi.fn().mockResolvedValue({ status: 'retry-scheduled' }),
    failFatal: vi.fn().mockResolvedValue({ status: 'failed-terminal' }),
    log: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue('applied'),
    getStats: vi.fn().mockResolvedValue({
      pending: 0,
      active: 0,
      completed: 0,
      failed: 0,
    } as QueueStats),
    findOne: vi.fn().mockResolvedValue(null),
    startup: vi.fn().mockResolvedValue(undefined),
    shutdown: vi.fn().mockResolvedValue(undefined),
    resetStorage: vi.fn().mockResolvedValue(undefined),
    ...over,
  } as unknown as IJobQueueBackend
}

/** Run one job through the processor and return the events it emitted. */
async function runOne(
  handler: Parameters<JobQueue['process']>[1],
  backendOver: Partial<IJobQueueBackend> = {},
): Promise<{ events: JobEvent[]; backend: IJobQueueBackend }> {
  const events: JobEvent[] = []
  let served = false
  const backend = makeBackend({
    claimNext: vi.fn().mockImplementation(async () => {
      if (served) return null
      served = true
      return aJob()
    }),
    ...backendOver,
  })
  const queue = new JobQueue(backend, silent, { onJobEvent: (e) => events.push(e) })
  queue.process('sync', handler, { pollInterval: 5 })
  await new Promise((r) => setTimeout(r, 60))
  await queue.shutdown(200)
  return { events, backend }
}

describe('JobQueue onJobEvent', () => {
  it('emits claimed then completed for a successful job', async () => {
    const { events } = await runOne(async () => {})
    expect(events.map((e) => e.kind)).toEqual(['claimed', 'completed'])
  })

  it('reports a plausible duration on completion', async () => {
    const { events } = await runOne(async () => {
      await new Promise((r) => setTimeout(r, 20))
    })
    const done = events.find((e) => e.kind === 'completed')
    expect(done && 'durationMs' in done && done.durationMs).toBeGreaterThanOrEqual(15)
  })

  it('marks a retryable failure as non-terminal', async () => {
    const { events } = await runOne(async () => {
      throw new Error('flaky')
    })
    const failed = events.find((e) => e.kind === 'failed')
    expect(failed).toMatchObject({ kind: 'failed', terminal: false, reason: 'flaky' })
  })

  it('marks a give-up as terminal, which is the distinction worth charting', async () => {
    const events: JobEvent[] = []
    let served = false
    const backend = makeBackend({
      claimNext: vi.fn().mockImplementation(async () => {
        if (served) return null
        served = true
        return aJob()
      }),
      fail: vi.fn().mockResolvedValue({ status: 'failed-terminal' }),
    })
    const queue = new JobQueue(backend, silent, {
      onJobEvent: (e) => events.push(e),
    })
    queue.process(
      'sync',
      async () => {
        throw new Error('dead')
      },
      { pollInterval: 5 },
    )
    await new Promise((r) => setTimeout(r, 60))
    await queue.shutdown(200)

    expect(events.find((e) => e.kind === 'failed')).toMatchObject({
      terminal: true,
    })
  })

  it('emits fail-fatal when a handler gives up explicitly', async () => {
    const { events } = await runOne(async () => {
      throw new FatalJobError('poison payload')
    })
    expect(events.find((e) => e.kind === 'fail-fatal')).toMatchObject({
      kind: 'fail-fatal',
      reason: 'poison payload',
    })
  })

  it('emits lease-lost with the operation that discovered it', async () => {
    const { events } = await runOne(
      async () => {},
      { complete: vi.fn().mockResolvedValue({ status: 'lease-lost' }) },
    )
    expect(events.find((e) => e.kind === 'lease-lost')).toMatchObject({
      kind: 'lease-lost',
      op: 'complete',
    })
  })

  it('does not fail the job when the sink throws', async () => {
    let served = false
    const backend = makeBackend({
      claimNext: vi.fn().mockImplementation(async () => {
        if (served) return null
        served = true
        return aJob()
      }),
    })
    const queue = new JobQueue(backend, silent, {
      onJobEvent: () => {
        throw new Error('bad metrics code')
      },
    })
    queue.process(
      'sync',
      async () => {},
      { pollInterval: 5 },
    )
    await new Promise((r) => setTimeout(r, 60))
    await queue.shutdown(200)

    // A bug in the consumer's metrics code must never cost them a job.
    expect(backend.complete).toHaveBeenCalled()
    expect(backend.fail).not.toHaveBeenCalled()
  })

  it('changes nothing when no sink is configured', async () => {
    let served = false
    const backend = makeBackend({
      claimNext: vi.fn().mockImplementation(async () => {
        if (served) return null
        served = true
        return aJob()
      }),
    })
    const queue = new JobQueue(backend, silent)
    queue.process(
      'sync',
      async () => {},
      { pollInterval: 5 },
    )
    await new Promise((r) => setTimeout(r, 60))
    await queue.shutdown(200)

    expect(backend.complete).toHaveBeenCalled()
  })

  it('emits reaper-recovered with a saturation hint', async () => {
    const events: JobEvent[] = []
    const backend = makeBackend({
      recoverStuckJobs: vi.fn().mockResolvedValue(3),
    })
    const queue = new JobQueue(backend, silent, {
      onJobEvent: (e) => events.push(e),
    })
    await queue.startReaper(5)
    await new Promise((r) => setTimeout(r, 40))
    queue.stopReaper()

    expect(events.find((e) => e.kind === 'reaper-recovered')).toMatchObject({
      kind: 'reaper-recovered',
      handled: 3,
      saturated: false,
    })
  })
})
