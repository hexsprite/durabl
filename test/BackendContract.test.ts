import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { EnqueueOptions, Job, StepRecord } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

interface BackendFixture {
  name: string
  inline: boolean
  mongoIdField: boolean
  make(): Promise<IJobQueueBackend>
  dispose(backend: IJobQueueBackend): Promise<void>
}

const fixtures: BackendFixture[] = [
  {
    name: 'DummyBackend',
    inline: false,
    mongoIdField: false,
    make: async () => new DummyBackend(),
    dispose: async (backend) => {
      await backend.resetStorage()
      await backend.shutdown()
    },
  },
  {
    name: 'ImmediateBackend',
    inline: true,
    mongoIdField: false,
    make: async () => new ImmediateBackend(),
    dispose: async (backend) => {
      await backend.resetStorage()
      await backend.shutdown()
    },
  },
  {
    name: 'MongoJobQueue',
    inline: false,
    mongoIdField: true,
    make: async () => {
      const { db } = await getMongo()
      const backend = new MongoJobQueue({
        db,
        collectionName: uniqueCollectionName('contract'),
      })
      await backend.startup()
      return backend
    },
    dispose: async (backend) => {
      await backend.shutdown()
      await (backend as MongoJobQueue)
        .getCollection()
        .drop()
        .catch(() => undefined)
    },
  },
]

function requireId(id: string | null): string {
  if (id === null) throw new Error('expected enqueue to create a job')
  return id
}

describe.each(fixtures)('backend contract: $name', (fixture) => {
  let backend: IJobQueueBackend

  beforeEach(async () => {
    backend = await fixture.make()
  })

  afterEach(async () => {
    vi.useRealTimers()
    await fixture.dispose(backend)
  })

  async function readJob<T = unknown>(id: string): Promise<Job<T>> {
    const query = fixture.mongoIdField ? { _id: id } : { id }
    const job = await backend.findOne<T>(query)
    if (!job) throw new Error(`expected job ${id} to exist`)
    return job
  }

  async function enqueueJob(
    type: string,
    data: unknown = {},
    options: EnqueueOptions = {},
  ): Promise<string> {
    return requireId(await backend.enqueue(type, data, options))
  }

  async function claimJob(type: string): Promise<Job> {
    const job = await backend.claimNext(type)
    if (!job) throw new Error(`expected a pending ${type} job to be claimed`)
    return job
  }

  function requireClaimToken(job: Job | null): string {
    if (!job?.claimToken) throw new Error('expected a live claim token')
    return job.claimToken
  }

  // Regression: DummyBackend.findOne used createdAt for runAt, hiding the
  // stored schedule from callers and retry-spacing assertions.
  it('findOne reports the stored runAt', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-01-02T03:04:05.000Z')
    vi.setSystemTime(now)
    const id = requireId(
      await backend.enqueue('projection', {}, { delay: 2_000 }),
    )

    const found = await readJob(id)
    const expectedDelay = fixture.inline ? 0 : 2_000
    expect(found.runAt).toEqual(new Date(now.getTime() + expectedDelay))
  })

  // Regression: DummyBackend.findOne omitted claimedAt, so lease-renewal
  // behavior could not be observed through the public job view.
  it('findOne reports the current claimedAt', async () => {
    const claimed = await backend.claimOrEnqueue('projection', {})
    expect(claimed).not.toBeNull()

    const found = await readJob(claimed!.id)
    expect(found.claimedAt).toEqual(claimed!.claimedAt)
  })

  // Regression: DummyBackend claimed insertion order, ignoring Mongo's
  // ascending-priority claim contract.
  it('claims due jobs by ascending priority', async () => {
    async function seedPending(priority: number): Promise<string> {
      const claim = await backend.claimOrEnqueue('priority', { priority }, {
        priority,
      })
      if (!claim) throw new Error('expected claimOrEnqueue to create a job')
      expect(await backend.release(claim.id, claim.claimToken)).toEqual({
        status: 'released',
      })
      return claim.id
    }

    await seedPending(5)
    const highestPriorityId = await seedPending(1)
    await seedPending(5)

    const claimed = await backend.claimNext('priority')
    expect(claimed?.id).toBe(highestPriorityId)
  })

  // Regression: in-memory failures made retries immediately claimable instead
  // of honoring the job's persisted retry spacing.
  it('spaces a scheduled retry by its fixed backoff', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-02-03T04:05:06.000Z')
    vi.setSystemTime(now)
    const options = {
      maxAttempts: 3,
      backoff: 'fixed' as const,
      backoffDelay: 5_000,
      backoffMaxDelay: 5_000,
    }
    const claimed = await backend.claimOrEnqueue('retry', {}, options)
    expect(claimed).not.toBeNull()

    expect(
      await backend.fail(claimed!.id, 'transient', claimed!.claimToken),
    ).toEqual({ status: 'retry-scheduled' })
    const retried = await readJob(claimed!.id)

    expect(retried.runAt).toEqual(new Date(now.getTime() + 5_000))
    expect(await backend.claimNext('retry')).toBeNull()
  })

  // Regression: memory journal appends did not renew the active run's lease.
  it('extends claimedAt after a fenced journal append', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-03-04T05:06:07.000Z')
    vi.setSystemTime(now)
    const claimed = await backend.claimOrEnqueue('journal', {})
    expect(claimed).not.toBeNull()
    const before = await readJob(claimed!.id)
    const token = requireClaimToken(claimed)
    const record: StepRecord = {
      seq: 0,
      name: 'saved result',
      result: { value: 42 },
      ts: now,
    }
    vi.setSystemTime(new Date(now.getTime() + 1_000))

    if (!backend.appendStep) throw new Error('journal append is required')
    expect(
      await backend.appendStep(claimed!.id, token, record),
    ).toEqual({ status: 'appended' })
    const after = await readJob(claimed!.id)

    expect(after.claimedAt!.getTime()).toBeGreaterThan(
      before.claimedAt!.getTime(),
    )
  })

  it('claims same-priority jobs by earliest runAt, not insertion order', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-04-05T06:07:08.000Z')
    vi.setSystemTime(now)
    const insertedFirst = await backend.claimOrEnqueue('runAt-order', {})
    const dueFirst = await backend.claimOrEnqueue('runAt-order', {})
    expect(insertedFirst).not.toBeNull()
    expect(dueFirst).not.toBeNull()

    vi.setSystemTime(new Date(now.getTime() + 1_000))
    expect(
      await backend.release(dueFirst!.id, dueFirst!.claimToken),
    ).toEqual({ status: 'released' })
    vi.setSystemTime(new Date(now.getTime() + 2_000))
    expect(
      await backend.release(insertedFirst!.id, insertedFirst!.claimToken),
    ).toEqual({ status: 'released' })

    expect((await claimJob('runAt-order')).id).toBe(dueFirst!.id)
  })

  // ImmediateBackend intentionally ignores delay; the other adapters enforce it.
  it('withholds delayed jobs until due, except for documented inline execution', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-05-06T07:08:09.000Z')
    vi.setSystemTime(now)
    const id = await enqueueJob('delayed', {}, { delay: 5_000 })

    if (fixture.inline) {
      expect((await readJob(id)).runAt).toEqual(now)
      expect((await claimJob('delayed')).id).toBe(id)
      return
    }

    expect(await backend.claimNext('delayed')).toBeNull()
    vi.setSystemTime(new Date(now.getTime() + 5_000))
    expect((await claimJob('delayed')).id).toBe(id)
  })

  it('returns null when no pending work exists', async () => {
    expect(await backend.claimNext('empty')).toBeNull()
  })

  it('completes an active run with its claim token', async () => {
    const id = await enqueueJob('complete', {})
    const claim = await claimJob('complete')

    expect(await backend.complete(id, requireClaimToken(claim))).toEqual({
      status: 'completed',
    })
    expect((await readJob(id)).status).toBe('completed')
  })

  it('rejects a stale completion without changing the active run', async () => {
    const id = await enqueueJob('stale-complete', {})
    const claim = await claimJob('stale-complete')
    const token = requireClaimToken(claim)

    expect(await backend.complete(id, `${token}:stale`)).toEqual({
      status: 'lease-lost',
    })
    expect((await readJob(id)).status).toBe('active')
    expect((await backend.getStats('stale-complete')).active).toBe(1)
    expect(await backend.complete(id, token)).toEqual({ status: 'completed' })
  })

  it('marks exhausted retries as terminal failures', async () => {
    const claim = await backend.claimOrEnqueue(
      'exhausted',
      {},
      { maxAttempts: 1 },
    )
    expect(claim).not.toBeNull()

    expect(
      await backend.fail(claim!.id, 'exhausted', claim!.claimToken),
    ).toEqual({ status: 'failed-terminal' })
    expect(
      (await backend.listFailed({ failureKind: 'retries-exhausted' })).map(
        (job) => job.id,
      ),
    ).toEqual([claim!.id])
  })

  it('marks fatal failures with the fatal failure kind', async () => {
    const claim = await backend.claimOrEnqueue('fatal', {})
    expect(claim).not.toBeNull()

    expect(
      await backend.failFatal(claim!.id, 'poison', claim!.claimToken),
    ).toEqual({ status: 'failed-terminal' })
    expect(
      (await backend.listFailed({ failureKind: 'fatal' })).map(
        (job) => job.id,
      ),
    ).toEqual([claim!.id])
  })

  it('releases an active run without consuming an attempt', async () => {
    const id = await enqueueJob('release', {})
    const claim = await claimJob('release')
    const attempt = claim.attempt

    expect(await backend.release(id, requireClaimToken(claim))).toEqual({
      status: 'released',
    })
    expect(await readJob(id)).toMatchObject({ status: 'pending', attempt })
    expect((await claimJob('release')).id).toBe(id)
  })

  it('blocks duplicates under pending+active dedupe scope', async () => {
    const options = { dedupeKey: 'one-live-run' }
    const id = await enqueueJob('dedupe-default', {}, options)

    expect(await backend.enqueue('dedupe-default', {}, options)).toBeNull()
    const claim = await claimJob('dedupe-default')
    expect(claim.id).toBe(id)
    expect(await backend.enqueue('dedupe-default', {}, options)).toBeNull()
  })

  it('allows one pending follower behind an active run in pending scope', async () => {
    const options = {
      dedupeKey: 'one-pending-follower',
      dedupeScope: 'pending' as const,
    }
    const active = await backend.claimOrEnqueue('follower', {}, options)
    expect(active).not.toBeNull()

    expect(await backend.claimOrEnqueue('follower', {}, options)).toBeNull()
    expect(await backend.claimOrEnqueue('follower', {}, options)).toBeNull()
    expect(await backend.getStats('follower')).toMatchObject({
      active: 1,
      pending: 1,
    })
  })

  // Regression: ImmediateBackend treated an active pending-scope reservation
  // as occupied pending capacity, rejecting the follower allowed by the API.
  it('enqueue accepts one pending-scope follower behind a claimNext run', async () => {
    const options = {
      dedupeKey: 'enqueue-follower',
      dedupeScope: 'pending' as const,
    }
    const predecessorId = await enqueueJob('enqueue-follower', {}, options)
    const predecessor = await claimJob('enqueue-follower')
    const followerId = await enqueueJob('enqueue-follower', {}, options)

    expect(await backend.enqueue('enqueue-follower', {}, options)).toBeNull()
    expect(await backend.claimNext('enqueue-follower')).toBeNull()
    expect(await backend.complete(
      predecessorId,
      requireClaimToken(predecessor),
    )).toEqual({ status: 'completed' })
    expect((await claimJob('enqueue-follower')).id).toBe(followerId)
  })

  it('reports outstanding work while pending or active, then clears it', async () => {
    const options = { dedupeKey: 'outstanding' }
    const id = await enqueueJob('outstanding', {}, options)

    expect(await backend.hasOutstanding('outstanding', 'outstanding')).toBe(
      true,
    )
    const claim = await claimJob('outstanding')
    expect(await backend.hasOutstanding('outstanding', 'outstanding')).toBe(
      true,
    )
    await backend.complete(id, requireClaimToken(claim))
    expect(await backend.hasOutstanding('outstanding', 'outstanding')).toBe(
      false,
    )
  })

  it('lists failed jobs newest first and applies the limit', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-06-07T08:09:10.000Z')
    vi.setSystemTime(now)
    const older = await backend.claimOrEnqueue('dead-letter', {})
    expect(older).not.toBeNull()
    await backend.failFatal(older!.id, 'older', older!.claimToken)

    vi.setSystemTime(new Date(now.getTime() + 1_000))
    const newer = await backend.claimOrEnqueue('dead-letter', {})
    expect(newer).not.toBeNull()
    await backend.failFatal(newer!.id, 'newer', newer!.claimToken)

    expect((await backend.listFailed({ limit: 1 })).map((job) => job.id)).toEqual([
      newer!.id,
    ])
  })

  it('replays a failed job to pending with its attempt reset', async () => {
    const claim = await backend.claimOrEnqueue('replay', {})
    expect(claim).not.toBeNull()
    await backend.failFatal(claim!.id, 'replay me', claim!.claimToken)

    expect(await backend.retry(claim!.id)).toBe(true)
    expect(await readJob(claim!.id)).toMatchObject({ status: 'pending', attempt: 0 })
    expect(await backend.listFailed({ type: 'replay' })).toEqual([])
  })

  it('refuses to replay an active run', async () => {
    const claim = await backend.claimOrEnqueue('active-replay', {})
    expect(claim).not.toBeNull()

    expect(await backend.retry(claim!.id)).toBe(false)
    expect((await backend.getStats('active-replay')).active).toBe(1)
  })

  it('counts pending, active, completed, and failed jobs', async () => {
    await enqueueJob('count-pending')
    await enqueueJob('count-active')
    await claimJob('count-active')
    await enqueueJob('count-completed')
    const completed = await claimJob('count-completed')
    await backend.complete(completed.id, requireClaimToken(completed))
    await enqueueJob('count-failed')
    const failed = await claimJob('count-failed')
    await backend.failFatal(failed.id, 'fatal', requireClaimToken(failed))

    expect(await backend.getStats()).toMatchObject({
      pending: 1,
      active: 1,
      completed: 1,
      failed: 1,
      superseded: 0,
      failedByKind: { fatal: 1 },
    })
  })

  it('reports the age of an overdue pending backlog', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-07-08T09:10:11.000Z')
    vi.setSystemTime(now)
    await enqueueJob('backlog')
    vi.setSystemTime(new Date(now.getTime() + 3_000))

    expect((await backend.getStats('backlog')).oldestPendingLagMs).toBe(3_000)
  })

  // Regression: ImmediateBackend left a no-processor job active forever.
  it('leaves a no-processor enqueue pending at attempt zero and claimable', async () => {
    const id = await enqueueJob('no-processor')

    expect(await readJob(id)).toMatchObject({ status: 'pending', attempt: 0 })
    expect(await backend.getStats('no-processor')).toMatchObject({
      pending: 1,
      active: 0,
    })
    expect((await claimJob('no-processor')).id).toBe(id)
  })

  it('keeps a no-processor enqueue reserved against duplicate work', async () => {
    const options = { dedupeKey: 'unhandled-reservation' }
    const id = await enqueueJob('no-processor-dedupe', {}, options)

    expect(await backend.enqueue('no-processor-dedupe', {}, options)).toBeNull()
    expect((await readJob(id)).status).toBe('pending')
  })

  it('rejects a stale journal append without changing the active run', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-08-09T10:11:12.000Z')
    vi.setSystemTime(now)
    const claim = await backend.claimOrEnqueue('stale-journal', {})
    expect(claim).not.toBeNull()
    const before = await readJob(claim!.id)
    const token = requireClaimToken(claim)
    vi.setSystemTime(new Date(now.getTime() + 1_000))

    if (!backend.appendStep || !backend.readSteps) {
      throw new Error('journal methods are required')
    }
    expect(
      await backend.appendStep(
        claim!.id,
        `${token}:stale`,
        { seq: 0, name: 'stale', result: 'rejected', ts: new Date() },
      ),
    ).toEqual({ status: 'lease-lost' })
    expect(await backend.readSteps(claim!.id)).toEqual([])
    expect(await readJob(claim!.id)).toMatchObject({
      status: 'active',
      claimedAt: before.claimedAt,
    })
  })

  // Mongo returns the original record for a same-sequence append and does not
  // renew the lease again; the in-memory adapters must keep that replay exact.
  it('returns the stored record for an idempotent journal append', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-09-10T11:12:13.000Z')
    vi.setSystemTime(now)
    const claim = await backend.claimOrEnqueue('journal-replay', {})
    expect(claim).not.toBeNull()
    const token = requireClaimToken(claim)
    const original: StepRecord = {
      seq: 0,
      name: 'saved result',
      result: { value: 42 },
      ts: now,
    }
    if (!backend.appendStep) throw new Error('journal append is required')
    expect(
      await backend.appendStep(claim!.id, token, original),
    ).toEqual({ status: 'appended' })
    const afterFirst = await readJob(claim!.id)

    vi.setSystemTime(new Date(now.getTime() + 5_000))
    expect(
      await backend.appendStep(claim!.id, token, {
        ...original,
        result: { value: 'replacement' },
        ts: new Date(now.getTime() + 5_000),
      }),
    ).toEqual({ status: 'already-recorded', existing: original })
    expect(await readJob(claim!.id)).toMatchObject({
      claimedAt: afterFirst.claimedAt,
    })
  })

  it('renews claimedAt on a fenced heartbeat', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const now = new Date('2026-10-11T12:13:14.000Z')
    vi.setSystemTime(now)
    const claim = await backend.claimOrEnqueue('heartbeat', {})
    expect(claim).not.toBeNull()
    const before = await readJob(claim!.id)
    const token = requireClaimToken(claim)
    vi.setSystemTime(new Date(now.getTime() + 1_000))

    if (!backend.heartbeatClaimed) {
      throw new Error('claimed heartbeat is required')
    }
    expect(
      await backend.heartbeatClaimed(claim!.id, token),
    ).toBe('heartbeated')
    expect((await readJob(claim!.id)).claimedAt!.getTime()).toBeGreaterThan(
      before.claimedAt!.getTime(),
    )
  })

  it('rejects a stale claimed completion without changing the active run', async () => {
    const claim = await backend.claimOrEnqueue('claimed-completion', {})
    expect(claim).not.toBeNull()

    if (!backend.completeClaimed) {
      throw new Error('claimed completion is required')
    }
    const token = requireClaimToken(claim)
    expect(
      await backend.completeClaimed(claim!.id, `${token}:stale`),
    ).toBe('lease-lost')
    expect((await readJob(claim!.id)).status).toBe('active')
    expect(
      await backend.completeClaimed(claim!.id, token),
    ).toBe('completed')
    expect((await readJob(claim!.id)).status).toBe('completed')
  })
})

afterAll(closeMongo)
