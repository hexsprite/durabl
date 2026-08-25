/**
 * Mixed-scope dedupe rejection (du-82x).
 *
 * The three unique dedupe partial indexes all key on {dedupeKey, dedupeScope},
 * so one key used under two scopes fell under different indexes and got ZERO
 * mutual exclusion between them — silently, exactly where the caller believes
 * they have some. Decision (a) from the bead: a dedupeKey names one logical
 * resource, so mixing scopes on a live key is a caller error and is rejected
 * with DedupeScopeConflictError. Once everything under the key is terminal,
 * the key is free again — history does not conflict.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import { DedupeScopeConflictError } from '../src/types'
import type { JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

describe('one dedupeKey may not be live under two scopes', () => {
  describe('MongoJobQueue', () => {
    let db: Db
    let backend: MongoJobQueue
    let collection: Collection<JobDoc>

    beforeEach(async () => {
      ;({ db } = await getMongo())
      backend = new MongoJobQueue({
        db,
        collectionName: uniqueCollectionName('scope_conflict_jobs'),
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

    it('enqueue rejects the second scope while the first is live', async () => {
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending+active',
        }),
      ).resolves.not.toBeNull()

      const err = await backend
        .enqueue('sync', {}, { dedupeKey: 'k', dedupeScope: 'pending' })
        .catch((e: unknown) => e)
      expect(err).toBeInstanceOf(DedupeScopeConflictError)
      expect((err as Error).message).toContain("'pending'")
      expect((err as Error).message).toContain("'pending+active'")
      expect((err as Error).message).toContain('k')

      // …and in the other direction.
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'other',
          dedupeScope: 'pending',
        }),
      ).resolves.not.toBeNull()
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'other',
          dedupeScope: 'pending+active',
        }),
      ).rejects.toBeInstanceOf(DedupeScopeConflictError)
    })

    it('claimOrEnqueue rejects the second scope while the first is live', async () => {
      const handle = await backend.claimOrEnqueue('sync', {}, {
        dedupeKey: 'k',
        dedupeScope: 'pending',
      })
      expect(handle).not.toBeNull()
      await expect(
        backend.claimOrEnqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending+active',
        }),
      ).rejects.toBeInstanceOf(DedupeScopeConflictError)
    })

    it('the same scope still dedupes silently (returns null)', async () => {
      await expect(
        backend.enqueue('sync', {}, { dedupeKey: 'k', dedupeScope: 'pending' }),
      ).resolves.not.toBeNull()
      // Ordinary dedupe: null, not an error.
      await expect(
        backend.enqueue('sync', {}, { dedupeKey: 'k', dedupeScope: 'pending' }),
      ).resolves.toBeNull()
    })

    it('once every job under the key is terminal, the mix is allowed again', async () => {
      const first = await backend.enqueue('sync', {}, {
        dedupeKey: 'k',
        dedupeScope: 'pending+active',
        maxAttempts: 1,
      })
      const claimed = await backend.claimNext('sync')
      await backend.complete(claimed!.id, claimed!.claimToken)

      // History no longer conflicts: the completed doc keeps its old scope.
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending',
        }),
      ).resolves.not.toBeNull()
      void first
    })

    it('jobs without a dedupeKey are untouched by the guard', async () => {
      for (let i = 0; i < 3; i++) {
        await expect(backend.enqueue('sync', {})).resolves.not.toBeNull()
      }
      expect(await collection.countDocuments({ type: 'sync' })).toBe(3)
    })
  })

  describe('DummyBackend', () => {
    let backend: DummyBackend

    beforeEach(() => {
      backend = new DummyBackend()
    })

    it('enqueue rejects the second scope while the first is live', async () => {
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending+active',
        }),
      ).resolves.not.toBeNull()
      // Async method: the conflict surfaces as a rejection.
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending',
        }),
      ).rejects.toBeInstanceOf(DedupeScopeConflictError)
    })

    it('claimOrEnqueue rejects the second scope while the first is live', async () => {
      await expect(
        backend.claimOrEnqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending',
        }),
      ).resolves.not.toBeNull()
      await expect(
        backend.claimOrEnqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending+active',
        }),
      ).rejects.toBeInstanceOf(DedupeScopeConflictError)
    })

    it('terminal history does not conflict', async () => {
      await backend.enqueue('sync', {}, { dedupeKey: 'k', maxAttempts: 1 })
      const job = backend.jobs.find((j) => j.dedupeKey === 'k')!
      job.status = 'failed'
      job.failedAt = new Date()

      // The failed job no longer holds the key: the other scope may use it.
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending',
        }),
      ).resolves.not.toBeNull()
    })
  })

  describe('ImmediateBackend', () => {
    let backend: ImmediateBackend

    beforeEach(() => {
      backend = new ImmediateBackend()
    })

    it('enqueue rejects the second scope while the first is live', async () => {
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending+active',
        }),
      ).resolves.not.toBeNull()
      // Async method: the conflict surfaces as a rejection.
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending',
        }),
      ).rejects.toBeInstanceOf(DedupeScopeConflictError)
    })

    it('claimOrEnqueue rejects the second scope while the first is live', async () => {
      await expect(
        backend.claimOrEnqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending',
        }),
      ).resolves.not.toBeNull()
      await expect(
        backend.claimOrEnqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending+active',
        }),
      ).rejects.toBeInstanceOf(DedupeScopeConflictError)
    })

    it('completion frees the key for the other scope', async () => {
      const handle = await backend.claimOrEnqueue('sync', {}, {
        dedupeKey: 'k',
        dedupeScope: 'pending+active',
      })
      await handle!.complete()
      await expect(
        backend.enqueue('sync', {}, {
          dedupeKey: 'k',
          dedupeScope: 'pending',
        }),
      ).resolves.not.toBeNull()
    })
  })
})
