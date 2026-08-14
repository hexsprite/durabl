import type {
  Collection,
  Db,
  Document,
  Filter,
  FindOneAndUpdateOptions,
  UpdateFilter,
} from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { EnqueueOptions, JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

type LatestOptions = EnqueueOptions & { coalesce: 'latest' }

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

describe('MongoJobQueue claimOrEnqueue latest coalescing', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('latest_jobs'),
    })
    collection = backend.getCollection()
    await backend.startup()
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    await backend.shutdown()
    await collection.drop().catch(() => {
      /* collection may already be gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it.each(invalidLatestOptions)(
    'rejects $name before mutating storage',
    async ({ options }) => {
      const error = await rejectionOf(
        backend.claimOrEnqueue('sync', { version: 1 }, options),
      )
      const stored = await collection.countDocuments()

      expect({ rejected: error instanceof Error, stored }).toEqual({
        rejected: true,
        stored: 0,
      })
    },
  )

  it('claims the first request and replaces only its pending follower', async () => {
    const options = latest()

    const first = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    const second = await backend.claimOrEnqueue('sync', { version: 2 }, options)
    const initialFollower = await collection.findOne({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })
    const third = await backend.claimOrEnqueue('sync', { version: 3 }, options)
    const active = await collection.findOne({
      dedupeKey: options.dedupeKey,
      status: 'active',
    })
    const pending = await collection.findOne({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })

    expect(first?.data).toEqual({ version: 1 })
    expect(second).toBeNull()
    expect(initialFollower?.data).toEqual({ version: 2 })
    expect(third).toBeNull()
    expect(active?.data).toEqual({ version: 1 })
    expect(pending?.data).toEqual({ version: 3 })
    expect(await collection.countDocuments({ dedupeKey: options.dedupeKey })).toBe(2)
  })

  it('does not replace a pending payload owned by another job type', async () => {
    const options = latest()
    await backend.claimOrEnqueue('type-a', { version: 1 }, options)
    await backend.claimOrEnqueue('type-a', { version: 2 }, options)

    await expect(
      backend.claimOrEnqueue('type-b', { version: 3 }, options),
    ).resolves.toBeNull()

    const pendingA = await collection.findOne({
      type: 'type-a',
      status: 'pending',
    })
    expect(pendingA?.data).toEqual({ version: 2 })
    expect(await collection.findOne({ type: 'type-b' })).toBeNull()
  })

  it('surfaces exhaustion instead of dropping a same-type latest payload', async () => {
    const options = latest()
    await backend.claimOrEnqueue('sync', { version: 1 }, options)
    const committedUpdate = collection.updateOne.bind(collection)
    const duplicate = {
      code: 11000,
      keyValue: { dedupeKey: options.dedupeKey },
      keyPattern: { dedupeKey: 1 },
    }
    let upsertAttempts = 0
    vi.spyOn(collection, 'updateOne').mockImplementation(
      async (filter, update, updateOptions) => {
        if (updateOptions?.upsert) {
          upsertAttempts++
          throw duplicate
        }
        return committedUpdate(filter, update, updateOptions)
      },
    )

    await expect(
      backend.claimOrEnqueue('sync', { version: 2 }, options),
    ).rejects.toBe(duplicate)
    expect(upsertAttempts).toBe(3)
  })

  it('preserves one active job and one pending follower under a concurrent burst', async () => {
    const options = latest()

    const handles = await Promise.all(
      Array.from({ length: 24 }, (_, version) =>
        backend.claimOrEnqueue('sync', { version }, options),
      ),
    )
    const stored = await collection
      .find({ dedupeKey: options.dedupeKey })
      .project<{ status: string; data: { version: number } }>({
        _id: 0,
        status: 1,
        data: 1,
      })
      .toArray()

    expect(handles.filter((handle) => handle !== null)).toHaveLength(1)
    expect(stored.filter((job) => job.status === 'active')).toHaveLength(1)
    expect(stored.filter((job) => job.status === 'pending')).toHaveLength(1)
    expect(stored).toHaveLength(2)
  })

  it('creates the next follower when replacement races the current follower claim', async () => {
    const options = latest()
    const active = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)
    await active!.complete()

    const realFindOneAndUpdate = collection.findOneAndUpdate.bind(collection)
    let markClaimed!: () => void
    let releaseClaim!: () => void
    const claimedInStorage = new Promise<void>((resolve) => {
      markClaimed = resolve
    })
    const mayReturnClaim = new Promise<void>((resolve) => {
      releaseClaim = resolve
    })
    vi.spyOn(collection, 'findOneAndUpdate').mockImplementationOnce(
      async (
        filter: Filter<JobDoc>,
        update: UpdateFilter<JobDoc> | Document[],
        options?: FindOneAndUpdateOptions,
      ) => {
        const claimed =
          options === undefined
            ? await realFindOneAndUpdate(filter, update)
            : await realFindOneAndUpdate(filter, update, options)
        markClaimed()
        await mayReturnClaim
        return claimed
      },
    )

    const claim = backend.claimNext<{ version: number }>('sync')
    await claimedInStorage
    const replacement = await backend
      .claimOrEnqueue('sync', { version: 3 }, options)
      .finally(() => releaseClaim())
    const claimed = await claim
    const pending = await collection.findOne({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })

    expect(claimed?.data).toEqual({ version: 2 })
    expect(replacement).toBeNull()
    expect(pending?.data).toEqual({ version: 3 })
    expect(
      await collection.countDocuments({
        dedupeKey: options.dedupeKey,
        status: 'active',
      }),
    ).toBe(1)
    expect(
      await collection.countDocuments({
        dedupeKey: options.dedupeKey,
        status: 'pending',
      }),
    ).toBe(1)
  })

  it('supersedes a failed active payload and leaves the latest follower claimable', async () => {
    const options = { ...latest(), maxAttempts: 1 }
    const active = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)
    await backend.claimOrEnqueue('sync', { version: 3 }, options)

    const result = await active!.fail('transient')

    const superseded = await collection.findOne({ _id: active!.id })
    const follower = await backend.claimNext<{ version: number }>('sync')
    const stats = await backend.getStats()

    expect(result).toEqual({ status: 'superseded' })
    expect(superseded?.status).toBe('superseded')
    expect(superseded?.data).toEqual({ version: 1 })
    expect(follower?.data).toEqual({ version: 3 })
    expect(stats.superseded).toBe(1)
  })

  it('supersedes an active payload when release finds a pending follower', async () => {
    const options = latest()
    const active = await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)

    await expect(active!.release()).resolves.toEqual({ status: 'superseded' })
    expect((await collection.findOne({ _id: active!.id }))?.status).toBe(
      'superseded',
    )
    expect(await backend.claimNext('sync')).not.toBeNull()
  })

  it('keeps the prior follower when storage rejects its replacement', async () => {
    const options = latest()
    await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)
    await db.command({
      collMod: collection.collectionName,
      validator: { 'data.version': { $ne: 3 } },
      validationLevel: 'strict',
      validationAction: 'error',
    })

    const error = await rejectionOf(
      backend.claimOrEnqueue('sync', { version: 3 }, options),
    )
    const pending = await collection.findOne({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })

    expect({ rejected: error instanceof Error, data: pending?.data }).toEqual({
      rejected: true,
      data: { version: 2 },
    })
  })

  it('keeps the first follower payload when latest coalescing is not requested', async () => {
    const options = {
      dedupeKey: 'account:1',
      dedupeScope: 'pending' as const,
    }

    await backend.claimOrEnqueue('sync', { version: 1 }, options)
    await backend.claimOrEnqueue('sync', { version: 2 }, options)
    await backend.claimOrEnqueue('sync', { version: 3 }, options)

    const pending = await collection.findOne({
      dedupeKey: options.dedupeKey,
      status: 'pending',
    })
    expect(pending?.data).toEqual({ version: 2 })
  })
})
