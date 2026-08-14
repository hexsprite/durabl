import { describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import { JobQueue } from '../src/JobQueue'
import type { Logger } from '../src/logger'
import type { EnqueueOptions } from '../src/types'

type LatestOptions = EnqueueOptions & { coalesce: 'latest' }
type InMemoryBackend = DummyBackend | ImmediateBackend

const silentLogger: Logger = {
  child: () => silentLogger,
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
}

const invalidLatestOptions: Array<{
  name: string
  options: LatestOptions
}> = [
  {
    name: 'without a dedupe key',
    options: { coalesce: 'latest' },
  },
  {
    name: 'without pending dedupe scope',
    options: { coalesce: 'latest', dedupeKey: 'account:1' },
  },
  {
    name: 'with pending+active dedupe scope',
    options: {
      coalesce: 'latest',
      dedupeKey: 'account:1',
      dedupeScope: 'pending+active',
    },
  },
]

function latest(dedupeKey = 'account:1'): LatestOptions {
  return { coalesce: 'latest', dedupeKey, dedupeScope: 'pending' }
}

async function rejectionOf(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => null,
    (error: unknown) => error,
  )
}

describe('JobQueue claimOrEnqueue latest coalescing', () => {
  it.each(invalidLatestOptions)(
    'rejects $name before the backend stores a job',
    async ({ options }) => {
      const backend = new DummyBackend()
      const queue = new JobQueue(backend, silentLogger)

      const error = await rejectionOf(
        queue.claimOrEnqueue('sync', { version: 1 }, options),
      )
      const stats = await backend.getStats()

      expect({ rejected: error instanceof Error, active: stats.active, pending: stats.pending }).toEqual({
        rejected: true,
        active: 0,
        pending: 0,
      })
    },
  )
})

const inMemoryBackends: Array<{
  name: string
  create: () => InMemoryBackend
}> = [
  { name: 'DummyBackend', create: () => new DummyBackend() },
  { name: 'ImmediateBackend', create: () => new ImmediateBackend() },
]

describe.each(inMemoryBackends)('$name claimOrEnqueue latest coalescing', ({ create }) => {
  it.each(invalidLatestOptions)(
    'rejects $name before storing a job',
    async ({ options }) => {
      const backend = create()

      const error = await rejectionOf(
        backend.claimOrEnqueue('sync', { version: 1 }, options),
      )
      const stats = await backend.getStats()

      expect({ rejected: error instanceof Error, active: stats.active, pending: stats.pending }).toEqual({
        rejected: true,
        active: 0,
        pending: 0,
      })
    },
  )

  it('keeps the active payload and replaces only the pending follower', async () => {
    const backend = create()
    const options = latest()

    const first = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    const second = await backend.claimOrEnqueue('sync', { version: 2 }, options)
    const initialFollower = await backend.findOne<{ version: number }>({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })
    const third = await backend.claimOrEnqueue('sync', { version: 3 }, options)
    const active = await backend.findOne<{ version: number }>({
      dedupeKey: options.dedupeKey,
      status: 'active',
    })
    const pending = await backend.findOne<{ version: number }>({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })

    expect(first?.data).toEqual({ version: 1 })
    expect(second).toBeNull()
    expect(initialFollower?.data).toEqual({ version: 2 })
    expect(third).toBeNull()
    expect(active?.data).toEqual({ version: 1 })
    expect(pending?.data).toEqual({ version: 3 })
  })

  it('does not replace a pending payload owned by another job type', async () => {
    const backend = create()
    const options = latest()

    await backend.claimOrEnqueue('type-a', { version: 1 }, options)
    await backend.claimOrEnqueue('type-a', { version: 2 }, options)
    await backend.claimOrEnqueue('type-b', { version: 3 }, options)

    const pendingA = await backend.findOne<{ version: number }>({
      type: 'type-a',
      status: 'pending',
    })
    const typeB = await backend.findOne({ type: 'type-b' })
    expect(pendingA?.data).toEqual({ version: 2 })
    expect(typeB).toBeNull()
  })

  it('keeps one active job and one pending follower under a concurrent burst', async () => {
    const backend = create()
    const options = latest()

    const handles = await Promise.all(
      Array.from({ length: 16 }, (_, version) =>
        backend.claimOrEnqueue('sync', { version }, options),
      ),
    )
    const stats = await backend.getStats()

    expect(handles.filter((handle) => handle !== null)).toHaveLength(1)
    expect({ active: stats.active, pending: stats.pending }).toEqual({
      active: 1,
      pending: 1,
    })
  })

  it('creates the next follower when replacement races the current follower claim', async () => {
    const backend = create()
    const options = latest()

    const active = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)
    await active!.complete()

    const [claimed, replacement] = await Promise.all([
      backend.claimNext<{ version: number }>('sync'),
      backend.claimOrEnqueue('sync', { version: 3 }, options),
    ])
    const pending = await backend.findOne<{ version: number }>({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })

    expect(claimed?.data).toEqual({ version: 2 })
    expect(replacement).toBeNull()
    expect(pending?.data).toEqual({ version: 3 })
  })

  it('supersedes a failed active payload and leaves the latest follower claimable', async () => {
    const backend = create()
    const options = { ...latest(), maxAttempts: 3 }

    const active = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)
    await backend.claimOrEnqueue('sync', { version: 3 }, options)
    const result = await active!.fail('transient')

    const superseded = await backend.findOne({ id: active!.id })
    const follower = await backend.claimNext<{ version: number }>('sync')
    const stats = await backend.getStats()

    expect(result).toEqual({ status: 'superseded' })
    expect(superseded?.status).toBe('superseded')
    expect(follower?.data).toEqual({ version: 3 })
    expect(stats.superseded).toBe(1)
  })

  it('supersedes an active payload when release finds a pending follower', async () => {
    const backend = create()
    const options = latest()
    const active = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)

    await expect(active!.release()).resolves.toEqual({ status: 'superseded' })
    expect((await backend.findOne({ id: active!.id }))?.status).toBe(
      'superseded',
    )
    expect(await backend.claimNext('sync')).not.toBeNull()
  })
})

describe('legacy first-payload coalescing', () => {
  it('keeps the first pending follower when latest coalescing is not requested', async () => {
    const backend = new DummyBackend()
    const options = {
      dedupeKey: 'account:1',
      dedupeScope: 'pending' as const,
    }

    await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)
    await backend.claimOrEnqueue('sync', { version: 3 }, options)

    expect(backend.getJobsByStatus('pending')).toHaveLength(1)
    expect(backend.getJobsByStatus('pending')[0].data).toEqual({ version: 2 })
  })
})
