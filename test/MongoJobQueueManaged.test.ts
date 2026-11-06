import type { Collection, Db, UpdateFilter } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import { JobQueue } from '../src/JobQueue'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'
import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

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
    // The claim counted attempt 1; release gives it back (du-v3l).
    expect(released).toMatchObject({
      status: 'pending',
      attempt: 0,
    })
    // runAt is server time: allow the offset's half-round-trip error.
    expect(released!.runAt.getTime()).toBeGreaterThanOrEqual(beforeRelease - 50)
    expect(released!.runAt.getTime()).toBeLessThanOrEqual(Date.now() + 50)
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

    // A replay of the release that already applied (du-bw6) returns its stored
    // receipt. A token with no receipt is still fenced out.
    await expect(
      backend.release(handle!.id, first!.claimToken),
    ).resolves.toEqual({ status: 'released' })
    await expect(
      backend.release(handle!.id, 'not-a-claim-token'),
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

  // Symptom: every rolling deploy during a long job used up one retry. Real
  // MongoJobQueue + JobQueue: shutdown mid-job must not spend the attempt.
  it('graceful shutdown mid-job leaves the stored attempt unchanged for the restart', async () => {
    const first = new JobQueue(backend, silentLogger)
    let started = false
    first.process(
      'long',
      async (_job, ctx) => {
        started = true
        await new Promise<void>((resolve) => {
          ctx.signal.addEventListener('abort', () => resolve(), { once: true })
        })
      },
      { pollInterval: 20 },
    )
    const id = await first.enqueue('long', {}, { maxAttempts: 3 })
    await waitUntil(() => started)

    await first.shutdown(0)

    expect(await collection.findOne({ _id: id! })).toMatchObject({
      status: 'pending',
      attempt: 0,
    })

    const second = new JobQueue(backend, silentLogger)
    const seen: number[] = []
    second.process(
      'long',
      async (job) => {
        seen.push(job.attempt)
      },
      { pollInterval: 20 },
    )
    await waitUntil(() => seen.length > 0)
    await second.shutdown(1000)
    expect(seen).toEqual([1])
  })

  // Symptom (du-bw6): JobQueue retries a lifecycle write that threw. When the
  // first attempt had already applied, the retry missed the `status: 'active'`
  // + claimToken filter and came back `lease-lost`. JobQueue then logged a
  // spurious lease-lost warning and never emitted the real outcome event.
  // A miss must return the stored receipt for this exact token and operation.
  describe('retry of an already-applied terminal write (du-bw6)', () => {
    const reclaim = async (type: string) => {
      await collection.updateMany({}, { $set: { runAt: new Date(0) } })
      return backend.claimNext(type)
    }

    it('complete() called again with the same token returns completed', async () => {
      await backend.enqueue('rt-complete', {})
      const claimed = (await backend.claimNext('rt-complete'))!
      const first = await backend.complete(claimed.id, claimed.claimToken)
      expect(first).toEqual({ status: 'completed' })
      await expect(
        backend.complete(claimed.id, claimed.claimToken),
      ).resolves.toEqual(first)
    })

    it('fail() retry-scheduled: same token again returns retry-scheduled, not lease-lost', async () => {
      await backend.enqueue('rt-fail', {}, { maxAttempts: 3 })
      const claimed = (await backend.claimNext('rt-fail'))!
      const first = await backend.fail(claimed.id, 'boom', claimed.claimToken)
      expect(first).toEqual({ status: 'retry-scheduled' })
      await expect(
        backend.fail(claimed.id, 'boom', claimed.claimToken),
      ).resolves.toEqual(first)
    })

    it('fail() failed-terminal: same token again returns failed-terminal', async () => {
      await backend.enqueue('rt-fail-term', {}, { maxAttempts: 1 })
      const claimed = (await backend.claimNext('rt-fail-term'))!
      const first = await backend.fail(claimed.id, 'boom', claimed.claimToken)
      expect(first).toEqual({ status: 'failed-terminal' })
      await expect(
        backend.fail(claimed.id, 'boom', claimed.claimToken),
      ).resolves.toEqual(first)
    })

    it('fail() superseded: same token again returns superseded', async () => {
      const active = (await backend.claimOrEnqueue('rt-fail-sup', {}, single('k')))!
      await backend.enqueue('rt-fail-sup', {}, single('k'))
      const first = await backend.fail(active.id, 'stale', active.claimToken)
      expect(first).toEqual({ status: 'superseded' })
      await expect(
        backend.fail(active.id, 'stale', active.claimToken),
      ).resolves.toEqual(first)
    })

    it('failFatal(): same token again returns failed-terminal', async () => {
      await backend.enqueue('rt-fatal', {})
      const claimed = (await backend.claimNext('rt-fatal'))!
      const first = await backend.failFatal(claimed.id, 'poison', claimed.claimToken)
      expect(first).toEqual({ status: 'failed-terminal' })
      await expect(
        backend.failFatal(claimed.id, 'poison', claimed.claimToken),
      ).resolves.toEqual(first)
    })

    it('release(): same token again returns released (the release unset the token)', async () => {
      await backend.enqueue('rt-release', {})
      const claimed = (await backend.claimNext('rt-release'))!
      const first = await backend.release(claimed.id, claimed.claimToken)
      expect(first).toEqual({ status: 'released' })
      await expect(
        backend.release(claimed.id, claimed.claimToken),
      ).resolves.toEqual(first)
    })

    it('release() superseded: same token again returns superseded', async () => {
      const active = (await backend.claimOrEnqueue('rt-rel-sup', {}, single('k')))!
      await backend.enqueue('rt-rel-sup', {}, single('k'))
      const first = await backend.release(active.id, active.claimToken)
      expect(first).toEqual({ status: 'superseded' })
      await expect(
        backend.release(active.id, active.claimToken),
      ).resolves.toEqual(first)
    })

    it('a different claim token still gets lease-lost after reclaim (fencing)', async () => {
      await backend.enqueue('rt-fence', {}, { maxAttempts: 5 })
      const a = (await backend.claimNext('rt-fence'))!
      await backend.release(a.id, a.claimToken)
      const b = (await reclaim('rt-fence'))!
      expect(b.claimToken).not.toBe(a.claimToken)

      // Token A holds a release receipt only: fail and failFatal have none.
      await expect(backend.fail(b.id, 'x', a.claimToken)).resolves.toEqual({
        status: 'lease-lost',
      })
      await expect(
        backend.failFatal(b.id, 'x', a.claimToken),
      ).resolves.toEqual({ status: 'lease-lost' })
      // A token that never existed is fenced for every operation.
      for (const op of ['complete', 'release'] as const) {
        await expect(backend[op](b.id, 'bogus-token')).resolves.toEqual({
          status: 'lease-lost',
        })
      }
      await expect(backend.fail(b.id, 'x', 'bogus-token')).resolves.toEqual({
        status: 'lease-lost',
      })
      expect((await collection.findOne({ _id: b.id }))?.status).toBe('active')
    })

    it("an old claim's receipt never answers a new claim", async () => {
      await backend.enqueue('rt-leak', {}, { maxAttempts: 5 })
      const a = (await backend.claimNext('rt-leak'))!
      await backend.fail(a.id, 'first', a.claimToken)
      const b = (await reclaim('rt-leak'))!
      await backend.complete(b.id, b.claimToken)

      // B never wrote a fail receipt. A's retry-scheduled must not leak in.
      await expect(backend.fail(b.id, 'late', b.claimToken)).resolves.toEqual({
        status: 'already-terminal',
        terminalStatus: 'completed',
      })
    })

    // End to end: the first fail() applies, the ack is lost, JobQueue retries.
    it('JobQueue emits failed once and no lease-lost when a fail() ack is lost', async () => {
      const warns: string[] = []
      const logger = {
        ...silentLogger,
        warn: (_obj: unknown, msg?: string) => {
          warns.push(msg ?? '')
        },
        child: () => logger,
      }
      const events: { kind: string }[] = []
      const queue = new JobQueue(backend, logger, {
        onJobEvent: (event) => events.push(event),
      })
      const realFail = backend.fail.bind(backend)
      const fail = vi
        .spyOn(backend, 'fail')
        .mockImplementationOnce(async (...args) => {
          await realFail(...args)
          throw new Error('network acknowledgement lost after commit')
        })
        .mockImplementation(realFail)
      let runs = 0
      queue.process(
        'rt-e2e',
        async () => {
          runs++
          throw new Error('handler failed')
        },
        { pollInterval: 20 },
      )
      try {
        const id = await queue.enqueue(
          'rt-e2e',
          {},
          { maxAttempts: 3, backoff: 'fixed', backoffDelay: 60_000 },
        )
        await waitUntil(() => fail.mock.calls.length >= 2)
        await waitUntil(() =>
          events.some((e) => e.kind === 'failed' || e.kind === 'lease-lost'),
        )
        expect(runs).toBe(1)
        expect(events.filter((e) => e.kind === 'failed')).toHaveLength(1)
        expect(events.filter((e) => e.kind === 'lease-lost')).toHaveLength(0)
        expect(warns.filter((m) => m.includes('lease lost'))).toHaveLength(0)
        expect((await collection.findOne({ _id: id! }))?.status).toBe('pending')
      } finally {
        await queue.shutdown(100)
      }
    })
  })
})
