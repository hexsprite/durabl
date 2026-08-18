/**
 * Regression coverage for du-qwa.
 *
 * Symptom: an 8-minute Mongo outage at Focuster produced 29 captured
 * exceptions — one per claim failure per processor type — although the backoff
 * retried every one of them. A failure that will be retried is a warn. Only a
 * sustained streak is an error.
 */
import { describe, expect, it, vi } from 'vitest'

import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { JobQueue } from '../src/JobQueue'
import type { Logger } from '../src/logger'
import type { Job, JobHandle, QueueStats } from '../src/types'

interface RecordingLogger extends Logger {
  warns: Array<[unknown, string | undefined]>
  errors: Array<[unknown, string | undefined]>
}

function recordingLogger(): RecordingLogger {
  const log: RecordingLogger = {
    warns: [],
    errors: [],
    debug: () => {},
    info: () => {},
    warn: (o, m) => {
      log.warns.push([o, m])
    },
    error: (o, m) => {
      log.errors.push([o, m])
    },
    child: () => log,
  }
  return log
}

function makeBackend(
  claimNext: IJobQueueBackend['claimNext'],
  recoverStuckJobs?: () => Promise<number>,
): IJobQueueBackend {
  return {
    enqueue: vi.fn().mockResolvedValue('id'),
    claimOrEnqueue: vi.fn().mockResolvedValue(null as JobHandle | null),
    claimNext,
    complete: vi.fn().mockResolvedValue('applied'),
    fail: vi.fn().mockResolvedValue('applied'),
    failFatal: vi.fn().mockResolvedValue('applied'),
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
    recoverStuckJobs,
  } as unknown as IJobQueueBackend
}

/** Drive one claim attempt directly, so the test does not wait out the backoff. */
async function claimOnce(queue: JobQueue, type: string): Promise<void> {
  const q = queue as unknown as {
    processors: Map<string, object>
    claimAndProcess(state: object): Promise<boolean>
  }
  await q.claimAndProcess(q.processors.get(type)!)
}

describe('claim failure logging (du-qwa)', () => {
  it('logs a single claim failure at warn, not error', async () => {
    const log = recordingLogger()
    const queue = new JobQueue(
      makeBackend(vi.fn().mockRejectedValue(new Error('connection reset'))),
      log,
    )
    queue.process('sync', async () => {}, { pollInterval: 60000 })

    await claimOnce(queue, 'sync')

    expect(log.errors).toHaveLength(0)
    expect(log.warns).toHaveLength(1)
    // No detail is lost: the error object still reaches the logger.
    expect((log.warns[0][0] as { err: Error }).err.message).toBe(
      'connection reset',
    )
    expect(log.warns[0][1]).toBe('error claiming next job')
    await queue.shutdown(100)
  })

  it('escalates to one error after five consecutive failures, and a success resets', async () => {
    const log = recordingLogger()
    const claimNext = vi.fn().mockRejectedValue(new Error('connection reset'))
    const queue = new JobQueue(makeBackend(claimNext), log)
    queue.process('sync', async () => {}, { pollInterval: 60000 })

    for (let i = 0; i < 5; i++) await claimOnce(queue, 'sync')
    expect(log.errors).toHaveLength(1)
    expect(log.warns).toHaveLength(4)

    // A sustained outage stays at one error, not one per attempt.
    for (let i = 0; i < 3; i++) await claimOnce(queue, 'sync')
    expect(log.errors).toHaveLength(1)

    // The backend answers again: the streak resets, so the next outage
    // escalates on its own schedule.
    claimNext.mockResolvedValueOnce(null as Job | null)
    await claimOnce(queue, 'sync')
    for (let i = 0; i < 4; i++) await claimOnce(queue, 'sync')
    expect(log.errors).toHaveLength(1)
    await claimOnce(queue, 'sync')
    expect(log.errors).toHaveLength(2)

    await queue.shutdown(100)
  })
})

describe('reaper sweep failure logging (du-qwa)', () => {
  it('logs a failed sweep at warn with the error attached', async () => {
    const log = recordingLogger()
    const recoverStuckJobs = vi
      .fn()
      .mockRejectedValue(new Error('topology destroyed'))
    const queue = new JobQueue(
      makeBackend(vi.fn().mockResolvedValue(null), recoverStuckJobs),
      log,
    )

    await queue.startReaper(60000)

    expect(log.errors).toHaveLength(0)
    expect(log.warns).toHaveLength(1)
    expect((log.warns[0][0] as { err: Error }).err.message).toBe(
      'topology destroyed',
    )
    await queue.shutdown(100)
  })
})
