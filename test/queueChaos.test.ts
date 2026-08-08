/**
 * Fault-injection chaos over the claim/lease invariants (du-btm Part B).
 *
 * Part A (queueProperties.test.ts) explores STATE-SPACE shapes: configurations
 * nobody assembled by hand. This explores FAILURE interleavings: what breaks
 * when a write is dropped, duplicated, delayed or thrown. The two find different
 * things and both are needed — du-pz9 was a Part A defect, whereas a lost
 * `complete()` is only reachable this way.
 *
 * Faults are injected at the backend seam rather than by killing processes; the
 * reasoning is on FaultyBackend. Everything is driven by a seeded PRNG so any
 * failure replays exactly — the seed is printed on failure, and the replay
 * property below proves that guarantee holds.
 */
import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { DedupeScope, JobDoc } from '../src/types'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'
import {
  type FaultProfile,
  makeFaultyBackend,
  makeRng,
} from './model/FaultyBackend'
import { checkInvariants, type ModelJob } from './model/queueModel'

/** Fixed seed set: deterministic, and finishes fast enough for every PR. */
const SEEDS = (process.env.CHAOS_SEEDS ?? '1,7,13,42,99')
  .split(',')
  .map((s) => Number(s.trim()))
  .filter((n) => Number.isFinite(n))

const PROFILE: FaultProfile = {
  rate: 0.35,
  kinds: ['drop', 'throw', 'duplicate', 'delay'],
  // The lifecycle writes. Deliberately not `startup`/`shutdown`: faulting those
  // tests the harness, not the queue.
  methods: ['enqueue', 'claimNext', 'complete', 'fail', 'heartbeat', 'log'],
  delayMs: 3,
}

const KEYS = ['k1', 'k2', 'k3']
const SCOPES: DedupeScope[] = ['pending', 'pending+active']

/**
 * Drive a fault-injected workload and return the invariant violations plus the
 * observed job set, so a caller can assert on both.
 */
async function chaosRun(
  db: Db,
  seed: number,
): Promise<{ violations: string[]; jobs: ModelJob[]; collection: Collection<JobDoc> }> {
  const real = new MongoJobQueue({
    db,
    collectionName: uniqueCollectionName('chaos_jobs'),
  })
  const collection = real.getCollection()
  await real.startup()

  const { backend } = makeFaultyBackend(real, PROFILE, seed)
  const rng = makeRng(seed ^ 0x5f3759df)

  for (let step = 0; step < 40; step++) {
    const key = KEYS[Math.floor(rng() * KEYS.length)]!
    const scope = SCOPES[Math.floor(rng() * SCOPES.length)]!
    const roll = rng()
    try {
      if (roll < 0.45) {
        await backend.enqueue('chaos', { step }, { dedupeKey: key, dedupeScope: scope })
      } else if (roll < 0.8) {
        const claimed = await backend.claimNext('chaos')
        if (claimed) {
          // Half the claims are abandoned without a terminal write — this is
          // what worker death actually looks like from the queue's side.
          if (rng() < 0.5) {
            await backend.complete(claimed.id, claimed.claimToken)
          }
        }
      } else {
        // Reaper sweep with a zero window so every active lease looks expired.
        await real.recoverStuckJobs(0)
      }
    } catch {
      // Injected faults surface as throws by design; the caller's job is to
      // stay consistent, which the invariant check below verifies.
    }
  }

  const jobs = (await collection.find({}).toArray()).map(toModelJob)
  await real.shutdown()
  return { violations: checkInvariants(jobs), jobs, collection }
}

describe('claim/lease invariants survive fault injection', () => {
  let db: Db
  let collection: Collection<JobDoc> | undefined

  beforeEach(async () => {
    ;({ db } = await getMongo())
  })

  afterEach(async () => {
    await collection?.drop().catch(() => {
      /* already gone */
    })
    collection = undefined
  })

  afterAll(async () => {
    await closeMongo()
  })

  for (const seed of SEEDS) {
    it(`holds every invariant under faults (seed ${seed})`, async () => {
      const run = await chaosRun(db, seed)
      collection = run.collection
      // On failure the seed is in the test name, which is the replay handle.
      expect(run.violations).toEqual([])
    })
  }

  it('never lets a dedupeKey hold two active jobs, under any seed', async () => {
    for (const seed of SEEDS) {
      const run = await chaosRun(db, seed)
      const active = run.jobs.filter((j) => j.status === 'active')
      const slots = active
        .filter((j) => j.dedupeKey)
        .map((j) => `${j.dedupeKey}|${j.dedupeScope}`)
      expect(new Set(slots).size).toBe(slots.length)
      await run.collection.drop().catch(() => {
        /* already gone */
      })
    }
  })

  it('replays a seed identically, which is what makes a failure actionable', async () => {
    const first = await chaosRun(db, 12345)
    await first.collection.drop().catch(() => {})
    const second = await chaosRun(db, 12345)
    await second.collection.drop().catch(() => {})

    // Same seed, same decisions: the observable shape of the run must match.
    const shape = (jobs: ModelJob[]) =>
      jobs
        .map((j) => `${j.type}|${j.dedupeKey}|${j.dedupeScope}|${j.status}`)
        .sort()
    expect(shape(second.jobs)).toEqual(shape(first.jobs))
  })
})

function toModelJob(doc: JobDoc): ModelJob {
  return {
    id: doc._id,
    type: doc.type,
    status: doc.status,
    attempt: doc.attempt,
    maxAttempts: doc.maxAttempts,
    priority: doc.priority,
    dedupeKey: doc.dedupeKey,
    dedupeScope: doc.dedupeScope,
    delayMs: 0,
    runAtMs: doc.runAt ? doc.runAt.getTime() : 0,
    executions: doc.attempt,
    terminalWrites: 0,
  }
}
