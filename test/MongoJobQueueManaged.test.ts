import type { Collection, Db, UpdateFilter } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('MongoJobQueue managed claims and release', () => {
  let db: Db
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  const single = (dedupeKey: string) => ({
    dedupeKey,
    dedupeScope: 'pending' as const,
  })

  beforeEach(async () => {
    ;({ db } = await getMongo())
    backend = new MongoJobQueue({
      db,
      collectionName: uniqueCollectionName('managed_jobs'),
    })
    collection = backend.getCollection()
    await backend.startup()
  })

  afterEach(async () => {
    await backend.shutdown()
    await collection.drop().catch(() => {
      /* already gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it('claims only a due pending follower with the requested type and key', async () => {
    await backend.enqueue('sync', { key: 'other' }, single('other'))
    const wanted = await backend.enqueue(
      'sync',
      { key: 'wanted' },
      single('wanted'),
    )
    await backend.enqueue(
      'sync',
      { key: 'later' },
      { ...single('later'), delay: 60_000 },
    )

    const claimed = await backend.claimNextByKey<{ key: string }>(
      'sync',
      'wanted',
    )

    expect(claimed).toMatchObject({
      id: wanted,
      type: 'sync',
      dedupeKey: 'wanted',
      data: { key: 'wanted' },
    })
    await expect(
      backend.claimNextByKey('sync', 'later'),
    ).resolves.toBeNull()
  })

  it('fenced-releases an active job to due pending without a failure', async () => {
    const handle = await backend.claimOrEnqueue(
      'sync',
      { n: 1 },
      single('account:1'),
    )
    const active = await collection.findOne({ _id: handle!.id })
    const beforeRelease = Date.now()

    await expect(
      backend.release(handle!.id, active!.claimToken),
    ).resolves.toEqual({ status: 'released' })

    const released = await collection.findOne({ _id: handle!.id })
    expect(released).toMatchObject({
      status: 'pending',
      attempt: 1,
    })
    expect(released!.runAt.getTime()).toBeGreaterThanOrEqual(beforeRelease)
    expect(released!.runAt.getTime()).toBeLessThanOrEqual(Date.now())
    expect(released!.failReason).toBeUndefined()
    expect(released!.failedAt).toBeUndefined()
  })

  it('supersedes an active claim when its release finds a follower', async () => {
    const handle = await backend.claimOrEnqueue(
      'sync',
      { n: 1 },
      single('account:1'),
    )
    const follower = await backend.enqueue(
      'sync',
      { n: 2 },
      single('account:1'),
    )
    const active = await collection.findOne({ _id: handle!.id })

    await expect(
      backend.release(handle!.id, active!.claimToken),
    ).resolves.toEqual({ status: 'superseded' })

    expect(await collection.findOne({ _id: handle!.id })).toMatchObject({
      status: 'superseded',
      claimToken: active!.claimToken,
    })
    expect(await collection.findOne({ _id: follower! })).toMatchObject({
      status: 'pending',
    })
  })

  it('reconciles a superseded release after acknowledgement loss', async () => {
    const handle = await backend.claimOrEnqueue(
      'sync',
      { n: 1 },
      single('account:1'),
    )
    await backend.enqueue('sync', { n: 2 }, single('account:1'))
    const committedUpdate = collection.updateOne.bind(collection)
    vi.spyOn(collection, 'updateOne').mockImplementation(
      async (filter, update, options) => {
        const result = await committedUpdate(filter, update, options)
        if ((update as UpdateFilter<JobDoc>).$set?.status === 'superseded') {
          throw new Error('network acknowledgement lost after commit')
        }
        return result
      },
    )

    await expect(handle!.release()).resolves.toEqual({
      status: 'superseded',
    })
    expect((await collection.findOne({ _id: handle!.id }))?.status).toBe(
      'superseded',
    )
  })

  it('a stale release cannot mutate the replacement claim', async () => {
    const handle = await backend.claimOrEnqueue(
      'sync',
      { n: 1 },
      single('account:1'),
    )
    const first = await collection.findOne({ _id: handle!.id })
    await backend.release(handle!.id, first!.claimToken)
    const replacement = await backend.claimNextByKey<{ n: number }>(
      'sync',
      'account:1',
    )
    const replacementDoc = await collection.findOne({ _id: replacement!.id })
    expect(replacementDoc!.claimToken).not.toBe(first!.claimToken)

    await expect(
      backend.release(handle!.id, first!.claimToken),
    ).resolves.toEqual({ status: 'lease-lost' })

    expect(await collection.findOne({ _id: handle!.id })).toMatchObject({
      status: 'active',
      claimToken: replacementDoc!.claimToken,
    })
  })

  it('classifies terminal and missing releases without changing storage', async () => {
    const handle = await backend.claimOrEnqueue('sync', { n: 1 })
    const active = await collection.findOne({ _id: handle!.id })
    await handle!.complete()

    await expect(
      backend.release(handle!.id, active!.claimToken),
    ).resolves.toMatchObject({ status: 'already-terminal' })
    await expect(
      backend.release('missing', 'missing-claim'),
    ).resolves.toEqual({ status: 'not-found' })
    expect(await collection.findOne({ _id: handle!.id })).toMatchObject({
      status: 'completed',
    })
  })

  it('leaves the follower pending across the completion-to-next-claim crash boundary', async () => {
    const active = await backend.claimOrEnqueue(
      'sync',
      { n: 1 },
      single('account:1'),
    )
    const followerId = await backend.enqueue(
      'sync',
      { n: 2 },
      single('account:1'),
    )

    await active!.complete()

    expect(await collection.findOne({ _id: active!.id })).toMatchObject({
      status: 'completed',
    })
    expect(await collection.findOne({ _id: followerId! })).toMatchObject({
      status: 'pending',
      data: { n: 2 },
    })
  })

  it('claims the same-key follower immediately after the holder completes', async () => {
    const active = await backend.claimOrEnqueue(
      'sync',
      { n: 1 },
      single('account:1'),
    )
    const followerId = await backend.enqueue(
      'sync',
      { n: 2 },
      single('account:1'),
    )
    await active!.complete()

    const follower = await backend.claimNextByKey<{ n: number }>(
      'sync',
      'account:1',
    )

    expect(follower).toMatchObject({
      id: followerId,
      type: 'sync',
      dedupeKey: 'account:1',
      data: { n: 2 },
    })
  })
})
