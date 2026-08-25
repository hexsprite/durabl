/**
 * Structural failure classification + dead-letter operations (du-jdu).
 *
 * Terminal `failed` used to be one state written by three different paths —
 * retries exhausted, fatal payloads, reaper give-ups — distinguishable only by
 * substring-matching `failReason`, with no way to list what died or replay it.
 * These suites pin the structural fix: `failureKind` distinguishes the paths,
 * `listFailed` is the dead-letter view, and `retry()` is the way back out.
 *
 * Note on handles: a claimed Job/JobHandle is a snapshot taken at claim time,
 * so failure markers are asserted by re-reading the backend's stored state,
 * never off the stale snapshot.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { FailureKind, Job, JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

/** Drive a claimed job to terminal failure through the ordinary retry path. */
async function exhaustRetries(
  backend: MongoJobQueue | DummyBackend | ImmediateBackend,
  type: string,
  options: { dedupeKey?: string } = {},
): Promise<void> {
  if (backend instanceof ImmediateBackend) {
    const handle = await backend.claimOrEnqueue(type, {}, {
      maxAttempts: 1,
      ...options,
    })
    expect(handle).not.toBeNull()
    expect(await handle!.fail('flaky upstream')).toEqual({
      status: 'failed-terminal',
    })
    return
  }
  await backend.enqueue(type, {}, { maxAttempts: 1, ...options })
  const claimed = await backend.claimNext(type)
  expect(claimed).not.toBeNull()
  expect(
    await backend.fail(claimed!.id, 'flaky upstream', claimed!.claimToken),
  ).toEqual({ status: 'failed-terminal' })
}

/** Drive a claimed job to terminal failure via the poison-payload path. */
async function failFatally(
  backend: MongoJobQueue | DummyBackend | ImmediateBackend,
  type: string,
): Promise<void> {
  if (backend instanceof ImmediateBackend) {
    const handle = await backend.claimOrEnqueue(type, {})
    expect(handle).not.toBeNull()
    expect(await handle!.failFatal('payload will never deserialize')).toEqual({
      status: 'failed-terminal',
    })
    return
  }
  await backend.enqueue(type, {})
  const claimed = await backend.claimNext(type)
  expect(claimed).not.toBeNull()
  expect(
    await backend.failFatal(
      claimed!.id,
      'payload will never deserialize',
      claimed!.claimToken,
    ),
  ).toEqual({ status: 'failed-terminal' })
}

describe('failureKind is structural, not textual', () => {
  describe('MongoJobQueue', () => {
    let db: Db
    let backend: MongoJobQueue
    let collection: Collection<JobDoc>

    beforeEach(async () => {
      ;({ db } = await getMongo())
      backend = new MongoJobQueue({
        db,
        collectionName: uniqueCollectionName('failure_kinds_jobs'),
      })
      collection = backend.getCollection()
      await backend.startup()
    })

    afterEach(async () => {
      vi.restoreAllMocks()
      await backend.shutdown()
      await collection.drop().catch(() => {
        /* already gone */
      })
    })

    afterAll(async () => {
      await closeMongo()
    })

    it('fail() on exhausted retries records retries-exhausted', async () => {
      await exhaustRetries(backend, 'sync')
      const doc = await collection.findOne({ status: 'failed' })
      expect(doc?.failureKind).toBe<FailureKind>('retries-exhausted')
    })

    it('failFatal() records fatal', async () => {
      await failFatally(backend, 'sync')
      const doc = await collection.findOne({ status: 'failed' })
      expect(doc?.failureKind).toBe<FailureKind>('fatal')
    })

    it('the reaper give-up path records stalled', async () => {
      await backend.enqueue('sync', {}, { maxAttempts: 1 })
      const claimed = await backend.claimNext('sync')
      expect(claimed).not.toBeNull()
      // Zero visibility window: every active lease looks expired to the reaper.
      await expect(backend.recoverStuckJobs(0)).resolves.toBeGreaterThanOrEqual(
        1,
      )
      const doc = await collection.findOne({ _id: claimed!.id })
      expect(doc?.status).toBe('failed')
      expect(doc?.failureKind).toBe<FailureKind>('stalled')
    })

    it('completing a reaper-failed orchestration clears failureKind', async () => {
      await backend.enqueue('orchestrated', {}, { maxAttempts: 1 })
      const claimed = await backend.claimNext('orchestrated')
      await backend.recoverStuckJobs(0)
      // The late worker finishes every step under its still-valid token.
      await backend.completeClaimed(claimed!.id, claimed!.claimToken!)
      const doc = await collection.findOne({ _id: claimed!.id })
      expect(doc?.status).toBe('completed')
      expect(doc?.failureKind).toBeUndefined()
    })

    it('listFailed filters by kind, type, since and limit', async () => {
      await exhaustRetries(backend, 'flaky-type')
      await failFatally(backend, 'poison-type')

      const all = await backend.listFailed()
      expect(all).toHaveLength(2)
      expect(all.map((j) => j.failureKind).sort()).toEqual([
        'fatal',
        'retries-exhausted',
      ])

      expect(
        (await backend.listFailed({ failureKind: 'fatal' })).map((j) => j.type),
      ).toEqual(['poison-type'])
      expect(
        (await backend.listFailed({ type: 'flaky-type' })).map(
          (j) => j.failureKind,
        ),
      ).toEqual(['retries-exhausted'])
      // Nothing failed before the queue existed.
      expect(
        await backend.listFailed({ since: new Date(Date.now() + 60_000) }),
      ).toHaveLength(0)
      expect(await backend.listFailed({ limit: 1 })).toHaveLength(1)
    })

    it('retry() returns a failed job to pending at attempt 0, claimable again', async () => {
      await exhaustRetries(backend, 'sync')
      const failed = await collection.findOne<{ _id: string }>({
        status: 'failed',
      })
      await expect(backend.retry(failed!._id)).resolves.toBe(true)

      const doc = await collection.findOne({ _id: failed!._id })
      expect(doc?.status).toBe('pending')
      expect(doc?.attempt).toBe(0)
      expect(doc?.failReason).toBeUndefined()
      expect(doc?.failedAt).toBeUndefined()
      expect(doc?.failureKind).toBeUndefined()

      const reclaimed = await backend.claimNext('sync')
      expect(reclaimed).not.toBeNull()
      expect(reclaimed!.attempt).toBe(1)
    })

    it('retry() appends a log line recording the manual replay', async () => {
      await exhaustRetries(backend, 'sync')
      const failed = await collection.findOne<{ _id: string }>({
        status: 'failed',
      })
      await backend.retry(failed!._id)
      const doc = await collection.findOne({ _id: failed!._id })
      expect(doc?.logs.some((l) => l.message.includes('Manual replay'))).toBe(
        true,
      )
    })

    it('retry() refuses live and completed jobs without mutating them', async () => {
      await backend.enqueue('sync', { v: 1 }, { backoffDelay: 60_000 })
      const active = await backend.claimNext('sync')
      expect(active).not.toBeNull()

      await expect(backend.retry(active!.id)).resolves.toBe(false)
      let doc = await collection.findOne({ _id: active!.id })
      expect(doc?.status).toBe('active')
      expect(doc?.attempt).toBe(1)

      await backend.complete(active!.id, active!.claimToken)
      await expect(backend.retry(active!.id)).resolves.toBe(false)
      doc = await collection.findOne({ _id: active!.id })
      expect(doc?.status).toBe('completed')
    })

    it('retry() into a dedupe collision skips instead of throwing', async () => {
      // The failing job holds dedupeKey 'k' until it fails terminally…
      await exhaustRetries(backend, 'sync', { dedupeKey: 'k' })
      // …which frees the slot for this pending follow-up.
      const followerId = await backend.enqueue('sync', {}, {
        dedupeKey: 'k',
        dedupeScope: 'pending+active',
      })
      expect(followerId).not.toBeNull()
      const failed = await collection.findOne({
        status: 'failed',
        dedupeKey: 'k',
      })
      expect(failed).not.toBeNull()
      // But two live jobs under one key are not. Skip-and-return-false.
      await expect(backend.retry(failed!._id as string)).resolves.toBe(false)
      expect(
        (await collection.findOne({ _id: followerId! }))?.status,
      ).toBe('pending')
    })

    it('legacy failed documents without failureKind stay listed and replayable', async () => {
      const legacy: JobDoc = {
        _id: 'legacy-doc',
        type: 'sync',
        data: {},
        status: 'failed',
        priority: 0,
        attempt: 3,
        maxAttempts: 3,
        runAt: new Date(),
        createdAt: new Date(),
        failedAt: new Date(),
        failReason: 'written before failureKind existed',
        logs: [],
      }
      await collection.insertOne(legacy)

      const listed = await backend.listFailed()
      expect(listed.map((j) => j.id)).toContain('legacy-doc')
      expect(
        listed.find((j) => j.id === 'legacy-doc')?.failureKind,
      ).toBeUndefined()
      await expect(backend.retry('legacy-doc')).resolves.toBe(true)
    })

    it('getStats breaks the failed count down by kind', async () => {
      await exhaustRetries(backend, 'sync')
      await failFatally(backend, 'sync')
      const stats = await backend.getStats()
      expect(stats.failed).toBe(2)
      expect(stats.failedByKind).toMatchObject({
        'retries-exhausted': 1,
        fatal: 1,
      })
    })
  })

  describe('DummyBackend', () => {
    let backend: DummyBackend

    beforeEach(() => {
      backend = new DummyBackend()
    })

    it('exhausted and fatal paths record distinct kinds', async () => {
      await exhaustRetries(backend, 'sync')
      await failFatally(backend, 'other')
      const kinds = backend.jobs.map((j) => j.failureKind).sort()
      expect(kinds).toEqual(['fatal', 'retries-exhausted'])
    })

    it('listFailed filters by kind and retry() replays', async () => {
      await exhaustRetries(backend, 'sync')
      await failFatally(backend, 'other')
      const flakyId = backend.jobs.find((j) => j.type === 'sync')!.id

      expect(
        (await backend.listFailed({ failureKind: 'fatal' })).map((j) => j.id),
      ).not.toContain(flakyId)
      expect(await backend.listFailed({ limit: 1 })).toHaveLength(1)

      await expect(backend.retry(flakyId)).resolves.toBe(true)
      const job = backend.jobs.find((j) => j.id === flakyId)!
      expect(job.status).toBe('pending')
      expect(job.attempt).toBe(0)
      expect(job.failureKind).toBeUndefined()
      // Claimable again through the normal path.
      await expect(backend.claimNext('sync')).resolves.not.toBeNull()
    })

    it('retry() refuses live jobs and honours dedupe collisions', async () => {
      await backend.enqueue('live', {}, { backoffDelay: 60_000 })
      const active = await backend.claimNext('live')
      await expect(backend.retry(active!.id)).resolves.toBe(false)

      // Fail a job holding the key, then queue a live follower under it.
      await exhaustRetries(backend, 'sync', { dedupeKey: 'k' })
      await backend.enqueue('sync', {}, {
        dedupeKey: 'k',
        dedupeScope: 'pending+active',
      })
      const colliding = backend.jobs.find((j) => j.status === 'failed')!
      expect(colliding.dedupeKey).toBe('k')
      await expect(backend.retry(colliding.id)).resolves.toBe(false)
    })
  })

  describe('ImmediateBackend', () => {
    let backend: ImmediateBackend

    beforeEach(() => {
      backend = new ImmediateBackend()
    })

    /** The failed job of a type, read back through the public query path. */
    const failedJob = async (type: string): Promise<Job> => {
      const job = await backend.findOne({ type, status: 'failed' })
      expect(job).not.toBeNull()
      return job!
    }

    it('exhausted and fatal paths record distinct kinds', async () => {
      await exhaustRetries(backend, 'sync')
      await failFatally(backend, 'other')
      expect((await failedJob('sync')).failureKind).toBe('retries-exhausted')
      expect((await failedJob('other')).failureKind).toBe('fatal')
    })

    it('listFailed filters and retry() replays a failed job', async () => {
      await exhaustRetries(backend, 'sync')
      await failFatally(backend, 'other')
      const flakyId = (await failedJob('sync')).id

      expect(
        (await backend.listFailed({ failureKind: 'retries-exhausted' })).map(
          (j) => j.id,
        ),
      ).toEqual([flakyId])

      await expect(backend.retry(flakyId)).resolves.toBe(true)
      const job = await backend.findOne({ id: flakyId })
      expect(job?.status).toBe('pending')
      expect(job?.attempt).toBe(0)
      expect(job?.failureKind).toBeUndefined()
      await expect(backend.claimNext('sync')).resolves.not.toBeNull()
    })

    it('retry() refuses live jobs and honours dedupe collisions', async () => {
      await backend.enqueue('live', {}, { backoffDelay: 60_000 })
      const live = await backend.findOne({ type: 'live', status: 'active' })
      expect(live).not.toBeNull()
      await expect(backend.retry(live!.id)).resolves.toBe(false)

      // Fail a job holding the key, then queue a live follower under it.
      await exhaustRetries(backend, 'sync', { dedupeKey: 'k' })
      await backend.enqueue('sync', {}, {
        dedupeKey: 'k',
        dedupeScope: 'pending+active',
      })
      const colliding = await failedJob('sync')
      expect(colliding.dedupeKey).toBe('k')
      await expect(backend.retry(colliding.id)).resolves.toBe(false)
    })
  })
})
