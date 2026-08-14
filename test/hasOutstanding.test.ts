import { afterAll, describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import { JobQueue } from '../src/JobQueue'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'
import { silentLogger } from './testLogger'

type InspectionSubject = IJobQueueBackend | JobQueue

interface InspectionHarness {
  backend: IJobQueueBackend
  subject: InspectionSubject
  seedPending(type: string, dedupeKey: string): Promise<void>
  seedActiveAndPending(
    type: string,
    dedupeKey: string,
  ): Promise<{ activeId: string }>
  cleanup(): Promise<void>
}

type InspectionHarnessFactory = () => Promise<InspectionHarness>

const options = (dedupeKey: string) => ({
  dedupeKey,
  dedupeScope: 'pending' as const,
})

async function inspect(
  subject: InspectionSubject,
  type: string,
  dedupeKey: string,
): Promise<boolean> {
  return subject.hasOutstanding(type, dedupeKey)
}

async function seedActiveAndPending(
  backend: IJobQueueBackend,
  type: string,
  dedupeKey: string,
): Promise<{ activeId: string }> {
  const active = await backend.claimOrEnqueue(type, {}, options(dedupeKey))
  expect(active).not.toBeNull()
  await expect(backend.enqueue(type, {}, options(dedupeKey))).resolves.toEqual(
    expect.any(String),
  )
  return { activeId: active!.id }
}

async function makeDummyHarness(): Promise<InspectionHarness> {
  const backend = new DummyBackend()
  return {
    backend,
    subject: backend,
    seedPending: async (type, dedupeKey) => {
      await backend.enqueue(type, {}, options(dedupeKey))
    },
    seedActiveAndPending: (type, dedupeKey) =>
      seedActiveAndPending(backend, type, dedupeKey),
    cleanup: async () => {},
  }
}

async function makeImmediateHarness(): Promise<InspectionHarness> {
  const backend = new ImmediateBackend()
  const queue = new JobQueue(backend, silentLogger)
  return {
    backend,
    subject: backend,
    seedPending: async (type, dedupeKey) => {
      queue.process(type, async () => {
        throw new Error('retry later')
      })
      await queue
        .enqueue(type, {}, { ...options(dedupeKey), maxAttempts: 2 })
        .catch(() => undefined)
    },
    seedActiveAndPending: async (type, dedupeKey) => {
      const active = await backend.claimOrEnqueue(
        type,
        {},
        options(dedupeKey),
      )
      expect(active).not.toBeNull()
      await expect(
        backend.claimOrEnqueue(type, {}, options(dedupeKey)),
      ).resolves.toBeNull()
      return { activeId: active!.id }
    },
    cleanup: async () => {
      await queue.shutdown(0)
      backend.reset()
    },
  }
}

async function makeJobQueueHarness(): Promise<InspectionHarness> {
  const backend = new DummyBackend()
  const queue = new JobQueue(backend, silentLogger)
  return {
    backend,
    subject: queue,
    seedPending: async (type, dedupeKey) => {
      await backend.enqueue(type, {}, options(dedupeKey))
    },
    seedActiveAndPending: (type, dedupeKey) =>
      seedActiveAndPending(backend, type, dedupeKey),
    cleanup: () => queue.shutdown(),
  }
}

async function makeMongoHarness(): Promise<
  InspectionHarness & { backend: MongoJobQueue }
> {
  const { db } = await getMongo()
  const backend = new MongoJobQueue({
    db,
    collectionName: uniqueCollectionName('outstanding_jobs'),
  })
  await backend.startup()
  return {
    backend,
    subject: backend,
    seedPending: async (type, dedupeKey) => {
      await backend.enqueue(type, {}, options(dedupeKey))
    },
    seedActiveAndPending: (type, dedupeKey) =>
      seedActiveAndPending(backend, type, dedupeKey),
    cleanup: async () => {
      await backend.shutdown()
      await backend
        .getCollection()
        .drop()
        .catch(() => {
          /* already gone */
        })
    },
  }
}
async function withHarness(
  makeHarness: InspectionHarnessFactory,
  test: (harness: InspectionHarness) => Promise<void>,
): Promise<void> {
  const harness = await makeHarness()
  try {
    await test(harness)
  } finally {
    await harness.cleanup()
  }
}


function outstandingContract(
  name: string,
  makeHarness: InspectionHarnessFactory,
): void {
  describe(`${name}.hasOutstanding`, () => {
    it('returns false when no pending or active job matches', () =>
      withHarness(makeHarness, async ({ subject }) => {
        await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(false)
      }))

    it('returns true for a pending job', () =>
      withHarness(makeHarness, async ({ subject, seedPending }) => {
        await seedPending('sync', 'account:1')
        await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(true)
      }))

    it('returns true for an active job', () =>
      withHarness(makeHarness, async ({ backend, subject }) => {
        await backend.claimOrEnqueue('sync', {}, options('account:1'))
        await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(true)
      }))

    it('stays true while either an active or queued follow-up remains', () =>
      withHarness(
        makeHarness,
        async ({ backend, subject, seedActiveAndPending }) => {
          const { activeId } = await seedActiveAndPending('sync', 'account:1')
          await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(true)

          await backend.complete(activeId)
          await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(true)

          const pending = await backend.claimNext('sync')
          expect(pending).not.toBeNull()
          await backend.complete(pending!.id)
          await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(
            false,
          )
        },
      ))

    it('returns false after completion', () =>
      withHarness(makeHarness, async ({ backend, subject }) => {
        const handle = await backend.claimOrEnqueue(
          'sync',
          {},
          options('account:1'),
        )
        expect(handle).not.toBeNull()
        await handle!.complete()

        await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(false)
      }))

    it('returns false after fatal failure', () =>
      withHarness(makeHarness, async ({ backend, subject }) => {
        const handle = await backend.claimOrEnqueue(
          'sync',
          {},
          options('account:1'),
        )
        expect(handle).not.toBeNull()
        await backend.failFatal(handle!.id, 'invalid input')

        await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(false)
      }))

    it('does not match the same dedupe key on another job type', () =>
      withHarness(makeHarness, async ({ subject, seedPending }) => {
        await seedPending('sync-a', 'account:1')
        await expect(inspect(subject, 'sync-b', 'account:1')).resolves.toBe(
          false,
        )
      }))

    it('stays true when a retryable failure returns the job to pending', () =>
      withHarness(makeHarness, async ({ backend, subject }) => {
        const handle = await backend.claimOrEnqueue(
          'sync',
          {},
          { ...options('account:1'), maxAttempts: 2 },
        )
        expect(handle).not.toBeNull()
        await backend.fail(handle!.id, 'transient')

        await expect(inspect(subject, 'sync', 'account:1')).resolves.toBe(true)
      }))
  })
}

outstandingContract('JobQueue', makeJobQueueHarness)
outstandingContract('DummyBackend', makeDummyHarness)
outstandingContract('ImmediateBackend', makeImmediateHarness)
outstandingContract('MongoJobQueue', makeMongoHarness)

describe('MongoJobQueue.hasOutstanding reaper transitions', () => {
  it('stays true when the reaper returns an active job to pending', async () => {
    const harness = await makeMongoHarness()
    try {
      const handle = await harness.backend.claimOrEnqueue(
        'sync',
        {},
        { ...options('account:1'), maxAttempts: 2 },
      )
      expect(handle).not.toBeNull()
      await harness.backend.getCollection().updateOne(
        { _id: handle!.id },
        { $set: { claimedAt: new Date(0) } },
      )

      await harness.backend.recoverStuckJobs(1000)

      await expect(inspect(harness.subject, 'sync', 'account:1')).resolves.toBe(
        true,
      )
    } finally {
      await harness.cleanup()
    }
  })

  it('returns false when the reaper fails an attempt-exhausted job', async () => {
    const harness = await makeMongoHarness()
    try {
      const handle = await harness.backend.claimOrEnqueue(
        'sync',
        {},
        { ...options('account:1'), maxAttempts: 1 },
      )
      expect(handle).not.toBeNull()
      await harness.backend.getCollection().updateOne(
        { _id: handle!.id },
        { $set: { claimedAt: new Date(0) } },
      )

      await harness.backend.recoverStuckJobs(1000)

      await expect(inspect(harness.subject, 'sync', 'account:1')).resolves.toBe(
        false,
      )
    } finally {
      await harness.cleanup()
    }
  })
})

afterAll(async () => {
  await closeMongo()
})
