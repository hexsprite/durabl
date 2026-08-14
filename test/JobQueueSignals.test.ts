/**
 * installSignalHandlers (du-9g0).
 *
 * shutdown() was already correct and already opt-in, and the first production
 * consumer never called it — it had a working DDP graceful-shutdown handler
 * right beside the queue and simply did not know a queue drain existed. So every
 * deploy killed in-flight jobs mid-handler, left them active until the
 * visibility timeout, burned an attempt each, and re-ran their side effects.
 *
 * That is an affordance problem, not a user mistake: the broken path was the
 * default and it was silent. These tests pin the properties that make the safe
 * path safe to adopt — it never exits the process, it never clobbers a host's
 * own listeners, and a second signal does not start a second drain.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'

import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { JobQueue } from '../src/JobQueue'
import type { Job, JobHandle, QueueStats } from '../src/types'

const silent = {
  child: () => silent,
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
}

function makeBackend(
  over: Partial<IJobQueueBackend> = {},
): IJobQueueBackend {
  return {
    enqueue: vi.fn().mockResolvedValue('id'),
    claimOrEnqueue: vi.fn().mockResolvedValue(null as JobHandle | null),
    claimNext: vi.fn().mockResolvedValue(null as Job | null),
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
    ...over,
  } as unknown as IJobQueueBackend
}

/** Signals used here are non-default so a stray handler cannot affect vitest. */
const SIG = 'SIGUSR2' as NodeJS.Signals

describe('JobQueue.installSignalHandlers', () => {
  const uninstalls: Array<() => void> = []

  afterEach(() => {
    for (const u of uninstalls.splice(0)) u()
    process.removeAllListeners(SIG)
  })

  it('installs nothing unless called', () => {
    const before = process.listenerCount(SIG)
    new JobQueue(makeBackend(), silent)
    // Constructing a queue must never mutate global process state.
    expect(process.listenerCount(SIG)).toBe(before)
  })

  it('drains the queue when the signal fires', async () => {
    const backend = makeBackend()
    const queue = new JobQueue(backend, silent)
    uninstalls.push(queue.installSignalHandlers({ signals: [SIG], timeoutMs: 50 }))

    process.emit(SIG)
    await queue.draining

    expect(backend.shutdown).toHaveBeenCalled()
  })

  it('does not exit the process', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation((() => {
      throw new Error('process.exit must not be called by the library')
    }) as never)
    try {
      const queue = new JobQueue(makeBackend(), silent)
      uninstalls.push(
        queue.installSignalHandlers({ signals: [SIG], timeoutMs: 50 }),
      )
      process.emit(SIG)
      await queue.draining
      // Exiting is the host application's call — a library that exits on your
      // behalf is unusable inside a framework with its own shutdown sequence.
      expect(exit).not.toHaveBeenCalled()
    } finally {
      exit.mockRestore()
    }
  })

  it('does not start a second drain on a second signal', async () => {
    const backend = makeBackend()
    const queue = new JobQueue(backend, silent)
    uninstalls.push(
      queue.installSignalHandlers({ signals: [SIG], timeoutMs: 50 }),
    )

    process.emit(SIG)
    const first = queue.draining
    process.emit(SIG)
    expect(queue.draining).toBe(first)

    await queue.draining
    expect(backend.shutdown).toHaveBeenCalledTimes(1)
  })

  it('leaves a host application listener intact on uninstall', () => {
    const hostHandler = vi.fn()
    process.on(SIG, hostHandler)
    try {
      const queue = new JobQueue(makeBackend(), silent)
      const uninstall = queue.installSignalHandlers({ signals: [SIG] })
      expect(process.listenerCount(SIG)).toBe(2)

      uninstall()

      // Removes exactly its own listener — never removeAllListeners, which
      // would silently break the host's shutdown.
      expect(process.listenerCount(SIG)).toBe(1)
      process.emit(SIG)
      expect(hostHandler).toHaveBeenCalled()
    } finally {
      process.removeListener(SIG, hostHandler)
    }
  })

  it('survives a backend that fails to shut down', async () => {
    const backend = makeBackend({
      shutdown: vi.fn().mockRejectedValue(new Error('backend gone')),
    })
    const queue = new JobQueue(backend, silent)
    uninstalls.push(
      queue.installSignalHandlers({ signals: [SIG], timeoutMs: 50 }),
    )

    process.emit(SIG)
    // A drain failure must not become an unhandled rejection that kills the
    // process harder than the signal already would.
    await expect(queue.draining).resolves.toBeUndefined()
  })

  it('waits for an in-flight job before resolving', async () => {
    let release: (() => void) | undefined
    const gate = new Promise<void>((r) => {
      release = r
    })
    const job: Job = {
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
    }
    let served = false
    const backend = makeBackend({
      claimNext: vi.fn().mockImplementation(async () => {
        if (served) return null
        served = true
        return job
      }),
    })
    const queue = new JobQueue(backend, silent)
    // Short poll interval: the default is 5s, which would outlast this test.
    queue.process(
      'sync',
      async () => {
        await gate
      },
      { pollInterval: 5 },
    )
    // Let the poll loop pick the job up.
    await new Promise((r) => setTimeout(r, 40))

    uninstalls.push(
      queue.installSignalHandlers({ signals: [SIG], timeoutMs: 2000 }),
    )
    process.emit(SIG)

    release?.()
    await queue.draining
    expect(backend.complete).toHaveBeenCalled()
  })
})
