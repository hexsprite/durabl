/**
 * Property-based model testing for the claim/dedupe/lease invariants (du-btm
 * Part A).
 *
 * Every existing suite asserts a scenario someone thought of. du-pz9 (one
 * contended dedupeKey stalling the whole processor) and du-cir (DummyBackend
 * ignoring runAt on claim) both lived in shipped code because nobody had
 * assembled that particular combination — nothing *failed*, the configuration
 * was simply untested. This generates the combinations instead.
 *
 * Runs against MongoJobQueue (the production path) and DummyBackend, so the
 * invariants are shown to be backend-agnostic rather than Mongo trivia.
 *
 * Seeds are fixed by default so this is deterministic and fast enough for every
 * PR. Override with CHAOS_RUNS for a longer soak.
 */
import fc from 'fast-check'
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import type { IJobQueueBackend } from '../src/backends/IJobQueueBackend'
import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { DedupeScope, Job, JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'
import { activeKeys, checkInvariants, type ModelJob } from './model/queueModel'

const RUNS = Number(process.env.CHAOS_RUNS ?? 40)

/** Small domains: collisions are where the interesting behaviour lives. */
const TYPES = ['alpha', 'beta']
const KEYS = ['k1', 'k2', 'k3']

const specArb = fc.record({
  type: fc.constantFrom(...TYPES),
  dedupeKey: fc.option(fc.constantFrom(...KEYS), { nil: undefined }),
  dedupeScope: fc.constantFrom<DedupeScope>('pending', 'pending+active'),
  priority: fc.integer({ min: 0, max: 3 }),
  delayMs: fc.constantFrom(0, 0, 0, 60_000),
  maxAttempts: fc.integer({ min: 1, max: 3 }),
})

interface Spec {
  type: string
  dedupeKey?: string
  dedupeScope: DedupeScope
  priority: number
  delayMs: number
  maxAttempts: number
}

type Op =
  | { kind: 'enqueue'; spec: Spec }
  | { kind: 'claim'; type: string }
  | { kind: 'complete' }
  | { kind: 'fail' }

const opArb = fc.oneof(
  { arbitrary: specArb.map((spec) => ({ kind: 'enqueue', spec }) as Op), weight: 4 },
  {
    arbitrary: fc
      .constantFrom(...TYPES)
      .map((type) => ({ kind: 'claim', type }) as Op),
    weight: 4,
  },
  { arbitrary: fc.constant({ kind: 'complete' } as Op), weight: 2 },
  { arbitrary: fc.constant({ kind: 'fail' } as Op), weight: 1 },
)

/**
 * Drive one generated scenario against a backend, checking invariants after
 * every operation. Returns the violations found (empty means the run held).
 */
async function runScenario(
  backend: IJobQueueBackend,
  ops: Op[],
  readAll: () => Promise<ModelJob[]>,
): Promise<string[]> {
  const violations: string[] = []
  /** Jobs this scenario currently holds a claim on. */
  const held: Job[] = []

  for (const op of ops) {
    try {
      switch (op.kind) {
        case 'enqueue':
          await backend.enqueue(op.spec.type, {}, op.spec)
          break
        case 'claim': {
          const claimed = await backend.claimNext(op.type)
          if (claimed) held.push(claimed)
          break
        }
        case 'complete': {
          const job = held.shift()
          if (job) await backend.complete(job.id, job.claimToken)
          break
        }
        case 'fail': {
          const job = held.shift()
          if (job) await backend.fail(job.id, 'generated failure', job.claimToken)
          break
        }
      }
    } catch (err) {
      // A throw from any of these is itself a finding: the documented contract
      // is that contention and retry-exhaustion are ordinary outcomes, not
      // exceptions. This is the du-pz9 class.
      violations.push(
        `${op.kind} threw: ${err instanceof Error ? err.message : String(err)}`,
      )
    }

    violations.push(...checkInvariants(await readAll()))
    if (violations.length) return violations
  }

  return violations
}

describe('claim/dedupe invariants hold over generated scenarios', () => {
  let db: Db
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
  })

  afterEach(async () => {
    await collection?.drop().catch(() => {
      /* already gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it('MongoJobQueue: no scenario breaks an invariant', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(opArb, { minLength: 4, maxLength: 24 }), async (ops) => {
        const backend = new MongoJobQueue({
          db,
          collectionName: uniqueCollectionName('prop_jobs'),
        })
        collection = backend.getCollection()
        await backend.startup()
        try {
          const readAll = async (): Promise<ModelJob[]> =>
            (await collection.find({}).toArray()).map(toModelJob)
          const violations = await runScenario(backend, ops, readAll)
          expect(violations).toEqual([])
        } finally {
          await backend.shutdown()
          await collection.drop().catch(() => {
            /* already gone */
          })
        }
      }),
      { numRuns: RUNS, endOnFailure: true },
    )
  })

  it('DummyBackend: no scenario breaks an invariant', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(opArb, { minLength: 4, maxLength: 24 }), async (ops) => {
        const backend = new DummyBackend()
        const readAll = async (): Promise<ModelJob[]> =>
          [...TYPES.flatMap((t) => backend.getJobsByType(t))].map(toModelJob)
        const violations = await runScenario(backend, ops, readAll)
        expect(violations).toEqual([])
      }),
      { numRuns: RUNS, endOnFailure: true },
    )
  })
})

describe('claimable work is never withheld (the du-pz9 property)', () => {
  let db: Db
  let collection: Collection<JobDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
  })

  afterEach(async () => {
    await collection?.drop().catch(() => {
      /* already gone */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it('serves a free key even when other keys are contended', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.uniqueArray(fc.constantFrom(...KEYS), { minLength: 1, maxLength: 3 }),
        async (contended) => {
          const backend = new MongoJobQueue({
            db,
            collectionName: uniqueCollectionName('withheld_jobs'),
          })
          collection = backend.getCollection()
          await backend.startup()
          try {
            for (const key of contended) {
              await backend.claimOrEnqueue(
                'alpha',
                {},
                { dedupeKey: key, dedupeScope: 'pending' },
              )
              await backend.enqueue(
                'alpha',
                {},
                { dedupeKey: key, dedupeScope: 'pending' },
              )
            }
            await backend.enqueue(
              'alpha',
              { free: true },
              { dedupeKey: 'free-key', dedupeScope: 'pending' },
            )

            const claimed = await backend.claimNext<{ free?: boolean }>('alpha')
            // The free job is claimable by the documented rules, so a backend
            // that returns null here is withholding work.
            expect(claimed?.data.free).toBe(true)

            const jobs = (await collection.find({}).toArray()).map(toModelJob)
            expect(checkInvariants(jobs)).toEqual([])
            // Sanity: the contended keys really are held.
            expect(activeKeys(jobs).size).toBeGreaterThanOrEqual(
              contended.length,
            )
          } finally {
            await backend.shutdown()
            await collection.drop().catch(() => {
              /* already gone */
            })
          }
        },
      ),
      { numRuns: 10, endOnFailure: true },
    )
  })
})

function toModelJob(doc: {
  _id?: string
  id?: string
  type: string
  status: string
  attempt: number
  maxAttempts: number
  priority: number
  dedupeKey?: string
  dedupeScope?: string
  runAt?: Date
}): ModelJob {
  return {
    id: (doc._id ?? doc.id) as string,
    type: doc.type,
    status: doc.status as ModelJob['status'],
    attempt: doc.attempt,
    maxAttempts: doc.maxAttempts,
    priority: doc.priority,
    dedupeKey: doc.dedupeKey,
    dedupeScope: doc.dedupeScope as DedupeScope | undefined,
    delayMs: 0,
    runAtMs: doc.runAt ? doc.runAt.getTime() : 0,
    executions: doc.attempt,
    // Terminal writes are not observable from the document alone; the
    // per-operation checks catch double-completion via status transitions.
    terminalWrites: 0,
  }
}
