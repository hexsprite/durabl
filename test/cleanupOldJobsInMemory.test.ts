/**
 * Retention (`cleanupOldJobs`) on the in-memory backends.
 *
 * Symptom the object form fixes: dead letters (failed/superseded) expired on
 * the same short clock as completed jobs, so `listFailed` and `retry` lost them
 * a week after the failure. Both test backends must follow the same rules as
 * MongoJobQueue, or a unit test certifies behaviour production does not have.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'

const DAY = 24 * 60 * 60 * 1000

const makers: Array<[string, () => IJobQueueBackend]> = [
  ['DummyBackend', () => new DummyBackend()],
  ['ImmediateBackend', () => new ImmediateBackend()],
]

describe.each(makers)('cleanupOldJobs on %s', (_name, make) => {
  let backend: IJobQueueBackend

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
    backend = make()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  /** One job in each terminal state, all created "now". */
  async function seedTerminal() {
    const completed = await backend.claimOrEnqueue('t', {}, { dedupeKey: 'c' })
    await completed!.complete()

    const failed = await backend.claimOrEnqueue('t', {}, { dedupeKey: 'f' })
    await failed!.failFatal('boom')

    const superseded = await backend.claimOrEnqueue(
      't',
      {},
      { dedupeKey: 's', dedupeScope: 'pending' },
    )
    // A queued follow-up turns the failure into `superseded`.
    await backend.claimOrEnqueue(
      't',
      {},
      { dedupeKey: 's', dedupeScope: 'pending' },
    )
    await superseded!.fail('stale')

    return {
      completed: completed!.id,
      failed: failed!.id,
      superseded: superseded!.id,
    }
  }

  async function statusOf(id: string): Promise<string | undefined> {
    const job = await backend.findOne<unknown>({ id })
    return job?.status
  }

  async function counts() {
    const s = await backend.getStats()
    return { completed: s.completed, failed: s.failed, superseded: s.superseded }
  }

  it('is callable through the interface after an existence check', async () => {
    expect(typeof backend.cleanupOldJobs).toBe('function')
    const removed = await backend.cleanupOldJobs!()
    expect(removed).toBe(0)
  })

  it('number form deletes completed, failed and superseded jobs older than the age', async () => {
    await seedTerminal()
    vi.setSystemTime(Date.now() + 8 * DAY)

    const removed = await backend.cleanupOldJobs!(7 * DAY)

    expect(removed).toBe(3)
    expect(await counts()).toEqual({ completed: 0, failed: 0, superseded: 0 })
  })

  it('no argument uses the 7 day default for every terminal state', async () => {
    await seedTerminal()
    vi.setSystemTime(Date.now() + 6 * DAY)
    expect(await backend.cleanupOldJobs!()).toBe(0)

    vi.setSystemTime(Date.now() + 2 * DAY)
    expect(await backend.cleanupOldJobs!()).toBe(3)
  })

  it('number form keeps terminal jobs newer than the age', async () => {
    await seedTerminal()
    vi.setSystemTime(Date.now() + 1 * DAY)

    expect(await backend.cleanupOldJobs!(7 * DAY)).toBe(0)
    expect(await counts()).toEqual({ completed: 1, failed: 1, superseded: 1 })
  })

  it('object form without deadLetterMaxAgeMs keeps failed and superseded jobs forever', async () => {
    const ids = await seedTerminal()
    vi.setSystemTime(Date.now() + 3650 * DAY)

    const removed = await backend.cleanupOldJobs!({ completedMaxAgeMs: DAY })

    expect(removed).toBe(1)
    expect(await statusOf(ids.completed)).toBeUndefined()
    expect(await statusOf(ids.failed)).toBe('failed')
    expect(await statusOf(ids.superseded)).toBe('superseded')
  })

  it('object form treats deadLetterMaxAgeMs null as keep forever', async () => {
    await seedTerminal()
    vi.setSystemTime(Date.now() + 365 * DAY)

    await backend.cleanupOldJobs!({ deadLetterMaxAgeMs: null })

    expect(await counts()).toEqual({ completed: 0, failed: 1, superseded: 1 })
  })

  it('object form deletes dead letters older than deadLetterMaxAgeMs and keeps completed on their own clock', async () => {
    const ids = await seedTerminal()
    vi.setSystemTime(Date.now() + 10 * DAY)

    const removed = await backend.cleanupOldJobs!({
      completedMaxAgeMs: 30 * DAY,
      deadLetterMaxAgeMs: 7 * DAY,
    })

    expect(removed).toBe(2)
    expect(await statusOf(ids.completed)).toBe('completed')
    expect(await statusOf(ids.failed)).toBeUndefined()
    expect(await statusOf(ids.superseded)).toBeUndefined()
  })

  it('never deletes pending or active jobs', async () => {
    const active = await backend.claimOrEnqueue('t', {}, { dedupeKey: 'a' })
    const pending = await backend.enqueue('t', {}, { delay: 1000 })
    vi.setSystemTime(Date.now() + 3650 * DAY)

    const removed = await backend.cleanupOldJobs!({
      completedMaxAgeMs: 0,
      deadLetterMaxAgeMs: 0,
    })

    expect(removed).toBe(0)
    expect(await statusOf(active!.id)).toBe('active')
    // ImmediateBackend starts a job inline on enqueue, so only assert it survives.
    expect(await statusOf(pending!)).toMatch(/^(pending|active)$/)
  })

  it.each([
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['negative', -1],
  ])('throws on a %s age and deletes nothing', async (_label, bad) => {
    await seedTerminal()
    vi.setSystemTime(Date.now() + 3650 * DAY)

    await expect(backend.cleanupOldJobs!(bad)).rejects.toThrow(/cleanupOldJobs/)
    await expect(
      backend.cleanupOldJobs!({ completedMaxAgeMs: bad }),
    ).rejects.toThrow(/completedMaxAgeMs/)
    await expect(
      backend.cleanupOldJobs!({ deadLetterMaxAgeMs: bad }),
    ).rejects.toThrow(/deadLetterMaxAgeMs/)

    expect(await counts()).toEqual({ completed: 1, failed: 1, superseded: 1 })
  })

  it('throws on a non-positive batchSize', async () => {
    await expect(backend.cleanupOldJobs!({ batchSize: 0 })).rejects.toThrow(
      /batchSize/,
    )
  })
})
