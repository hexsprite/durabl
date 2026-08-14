import { describe, expect, it, type MockInstance, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import {
  createJobQueue,
  getDefaultQueue,
  getGlobalBackend,
  type JobQueue,
  setGlobalBackend,
  withGlobalQueue,
} from '../src/JobQueue'
import type { JobEvent } from '../src/types'

import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

function makeLogger() {
  const child = vi.fn()
  const logger = { ...silentLogger, child }
  child.mockReturnValue(logger)
  return logger
}

async function runOneJob(
  queue: JobQueue,
  type: string,
  events: JobEvent[],
): Promise<void> {
  queue.process(type, async () => {}, { pollInterval: 1000 })
  try {
    await queue.enqueue(type, {})
    await waitUntil(
      () => events.some((event) => event.kind === 'completed'),
      { timeoutMs: 500 },
    )
  } finally {
    await queue.shutdown(0)
  }
}

describe('JobQueue global helpers', () => {
  it('uses the logger, visibility timeout, and event sink captured by setGlobalBackend', async () => {
    const backend = new DummyBackend()
    const logger = makeLogger()
    const events: JobEvent[] = []

    setGlobalBackend(backend, {
      logger,
      visibilityTimeoutMs: 1234,
      onJobEvent: (event) => events.push(event),
    })

    const queue = getDefaultQueue()
    expect(getGlobalBackend()).toBe(backend)
    expect(queue.visibilityTimeoutMs).toBe(1234)
    expect(logger.child).toHaveBeenCalledWith({ category: 'JobQueue' })

    await runOneJob(queue, 'default-options', events)
    expect(events.map((event) => event.kind)).toEqual(['claimed', 'completed'])
  })

  it('inherits captured options when createJobQueue uses the global backend', async () => {
    const backend = new DummyBackend()
    const logger = makeLogger()
    const events: JobEvent[] = []

    setGlobalBackend(backend, {
      logger,
      visibilityTimeoutMs: 2345,
      onJobEvent: (event) => events.push(event),
    })

    const queue = createJobQueue()
    expect(queue).not.toBe(getDefaultQueue())
    expect(queue.visibilityTimeoutMs).toBe(2345)
    expect(logger.child).toHaveBeenCalledTimes(2)

    await runOneJob(queue, 'inherited-options', events)
    expect(events.map((event) => event.kind)).toEqual(['claimed', 'completed'])
  })

  it('merges explicit backend and option overrides over captured options', async () => {
    const capturedBackend = new DummyBackend()
    const capturedLogger = makeLogger()
    const capturedEvents: JobEvent[] = []
    setGlobalBackend(capturedBackend, {
      logger: capturedLogger,
      visibilityTimeoutMs: 3456,
      onJobEvent: (event) => capturedEvents.push(event),
    })

    const visibilityBackend = new DummyBackend()
    const visibilityQueue = createJobQueue(visibilityBackend, {
      visibilityTimeoutMs: 4567,
    })
    expect(visibilityQueue.visibilityTimeoutMs).toBe(4567)
    expect(capturedLogger.child).toHaveBeenCalledTimes(2)
    await runOneJob(visibilityQueue, 'partial-options', capturedEvents)

    const explicitBackend = new DummyBackend()
    const explicitLogger = makeLogger()
    const explicitEvents: JobEvent[] = []
    const explicitQueue = createJobQueue(explicitBackend, {
      logger: explicitLogger,
      onJobEvent: (event) => explicitEvents.push(event),
    })

    expect(getGlobalBackend()).toBe(capturedBackend)
    expect(explicitQueue.visibilityTimeoutMs).toBe(3456)
    expect(explicitLogger.child).toHaveBeenCalledWith({ category: 'JobQueue' })
    expect(capturedLogger.child).toHaveBeenCalledTimes(2)

    await runOneJob(explicitQueue, 'explicit-options', explicitEvents)
    expect(capturedEvents.map((event) => event.kind)).toEqual([
      'claimed',
      'completed',
    ])
    expect(explicitEvents.map((event) => event.kind)).toEqual([
      'claimed',
      'completed',
    ])
    expect(visibilityBackend.jobs).toHaveLength(1)
    expect(explicitBackend.jobs).toHaveLength(1)
    expect(capturedBackend.jobs).toHaveLength(0)
  })

  it('restores the exact prior backend and queue after a successful scope', async () => {
    const priorBackend = new DummyBackend()
    setGlobalBackend(priorBackend)
    const priorQueue = getDefaultQueue()
    const priorShutdown = vi.spyOn(priorQueue, 'shutdown')

    const temporaryBackend = new DummyBackend()
    let temporaryQueue!: JobQueue
    let temporaryShutdown!: MockInstance<JobQueue['shutdown']>

    const result = await withGlobalQueue(
      temporaryBackend,
      { visibilityTimeoutMs: 5678 },
      async () => {
        temporaryQueue = getDefaultQueue()
        temporaryShutdown = vi.spyOn(temporaryQueue, 'shutdown')
        expect(getGlobalBackend()).toBe(temporaryBackend)
        expect(temporaryQueue).not.toBe(priorQueue)
        expect(temporaryQueue.visibilityTimeoutMs).toBe(5678)
        return 'callback result'
      },
    )

    expect(result).toBe('callback result')
    expect(getGlobalBackend()).toBe(priorBackend)
    expect(getDefaultQueue()).toBe(priorQueue)
    expect(temporaryShutdown).toHaveBeenCalledTimes(1)
    expect(priorShutdown).not.toHaveBeenCalled()
  })

  it('uses default options when the scope receives only a callback', async () => {
    const priorBackend = new DummyBackend()
    setGlobalBackend(priorBackend, { visibilityTimeoutMs: 1234 })
    const temporaryBackend = new DummyBackend()

    const result = await withGlobalQueue(temporaryBackend, () => {
      expect(getGlobalBackend()).toBe(temporaryBackend)
      expect(getDefaultQueue().visibilityTimeoutMs).toBe(300000)
      return 'default callback result'
    })

    expect(result).toBe('default callback result')
    expect(getGlobalBackend()).toBe(priorBackend)
    expect(getDefaultQueue().visibilityTimeoutMs).toBe(1234)
  })

  it('restores the exact prior backend and queue when a scope throws', async () => {
    const priorBackend = new DummyBackend()
    setGlobalBackend(priorBackend)
    const priorQueue = getDefaultQueue()
    const priorShutdown = vi.spyOn(priorQueue, 'shutdown')

    const temporaryBackend = new DummyBackend()
    let temporaryShutdown!: MockInstance<JobQueue['shutdown']>
    const callbackError = new Error('callback failed')

    await expect(
      withGlobalQueue(temporaryBackend, {}, async () => {
        temporaryShutdown = vi.spyOn(getDefaultQueue(), 'shutdown')
        throw callbackError
      }),
    ).rejects.toBe(callbackError)

    expect(getGlobalBackend()).toBe(priorBackend)
    expect(getDefaultQueue()).toBe(priorQueue)
    expect(temporaryShutdown).toHaveBeenCalledTimes(1)
    expect(priorShutdown).not.toHaveBeenCalled()
  })

  it('rejects a nested scope before changing the active scope', async () => {
    const priorBackend = new DummyBackend()
    setGlobalBackend(priorBackend)
    const priorQueue = getDefaultQueue()

    const outerBackend = new DummyBackend()
    const nestedBackend = new DummyBackend()
    const nestedShutdown = vi.spyOn(nestedBackend, 'shutdown')

    await withGlobalQueue(outerBackend, {}, async () => {
      const outerQueue = getDefaultQueue()

      await expect(
        withGlobalQueue(nestedBackend, {}, async () => {}),
      ).rejects.toThrow()

      expect(getGlobalBackend()).toBe(outerBackend)
      expect(getDefaultQueue()).toBe(outerQueue)
    })

    expect(getGlobalBackend()).toBe(priorBackend)
    expect(getDefaultQueue()).toBe(priorQueue)
    expect(nestedShutdown).not.toHaveBeenCalled()
  })

  it('rejects concurrent overlap before changing the active scope', async () => {
    const priorBackend = new DummyBackend()
    setGlobalBackend(priorBackend)
    const priorQueue = getDefaultQueue()

    let release!: () => void
    const hold = new Promise<void>((resolve) => {
      release = resolve
    })
    let entered!: () => void
    const didEnter = new Promise<void>((resolve) => {
      entered = resolve
    })

    const firstBackend = new DummyBackend()
    let firstQueue!: JobQueue
    const firstScope = withGlobalQueue(firstBackend, {}, async () => {
      firstQueue = getDefaultQueue()
      entered()
      await hold
    })
    await didEnter

    const overlappingBackend = new DummyBackend()
    const overlappingShutdown = vi.spyOn(overlappingBackend, 'shutdown')
    await expect(
      withGlobalQueue(overlappingBackend, {}, async () => {}),
    ).rejects.toThrow()

    expect(getGlobalBackend()).toBe(firstBackend)
    expect(getDefaultQueue()).toBe(firstQueue)
    expect(overlappingShutdown).not.toHaveBeenCalled()

    release()
    await firstScope
    expect(getGlobalBackend()).toBe(priorBackend)
    expect(getDefaultQueue()).toBe(priorQueue)
  })
})
