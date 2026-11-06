/**
 * Server-clock offset measurement.
 *
 * Lease and schedule times come from the Mongo server clock (see
 * MongoJobQueueClockSkew.test.ts for the double-run symptom). The offset must
 * come from the round-trip midpoint, and a failed refresh must keep the last
 * good offset rather than fall back to the raw local clock.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ServerClock, type HelloReply } from '../src/backends/serverClock'
import type { Logger } from '../src/logger'

import { silentLogger } from './testLogger'

/** A local clock that returns the given readings in order, then the last. */
function readings(...values: number[]): () => number {
  let i = 0
  return () => values[Math.min(i++, values.length - 1)]!
}

describe('ServerClock', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('measures the offset from the round-trip midpoint', async () => {
    // Sent at 1000, received at 1100: midpoint 1050. Server said 6050.
    const clock = new ServerClock({
      hello: async () => ({ localTime: new Date(6050) }),
      logger: silentLogger,
      localNow: readings(1000, 1100, 2000),
    })
    await clock.refresh()
    expect(clock.offsetMs).toBe(5000)
    expect(clock.now().getTime()).toBe(7000)
  })

  it('keeps the previous offset and warns when a refresh fails', async () => {
    const warn = vi.fn()
    const logger = { ...silentLogger, warn } as Logger
    const replies: Array<() => Promise<HelloReply>> = [
      async () => ({ localTime: new Date(10_000) }),
      async () => {
        throw new Error('network down')
      },
      async () => ({}),
    ]
    const clock = new ServerClock({
      hello: () => replies.shift()!(),
      logger,
      localNow: () => 4000,
    })

    await clock.refresh()
    expect(clock.offsetMs).toBe(6000)

    await clock.refresh()
    expect(clock.offsetMs).toBe(6000)
    expect(warn).toHaveBeenCalledTimes(1)

    // A reply without localTime is a failed measurement too.
    await clock.refresh()
    expect(clock.offsetMs).toBe(6000)
    expect(warn).toHaveBeenCalledTimes(2)
  })

  it('re-measures on its interval until stopped', async () => {
    vi.useFakeTimers()
    const hello = vi.fn(async () => ({ localTime: new Date() }))
    const clock = new ServerClock({
      hello,
      logger: silentLogger,
      refreshIntervalMs: 1000,
    })
    await clock.start()
    expect(hello).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(2000)
    expect(hello).toHaveBeenCalledTimes(3)
    clock.stop()
    await vi.advanceTimersByTimeAsync(5000)
    expect(hello).toHaveBeenCalledTimes(3)
    expect(vi.getTimerCount()).toBe(0)
  })
})
