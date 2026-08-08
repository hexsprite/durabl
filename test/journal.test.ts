/**
 * Unit tests for the journal primitives: serialization guards, void sentinel,
 * size guard, in-memory lease-fenced append/complete/heartbeat, and the
 * deterministic now()/uuid() derivations. No Mongo.
 */
import { describe, expect, it } from 'vitest'

import {
  JournalTooLarge,
  NondeterminismError,
  NonSerializableStepResult,
} from '../src/journal/errors'
import {
  appendStepInMemory,
  completeClaimedInMemory,
  heartbeatClaimedInMemory,
  type JournalableJob,
} from '../src/journal/inMemory'
import {
  approxRecordBytes,
  assertSerializable,
  assertStepMatches,
  fromStored,
  guardAppend,
  toStored,
} from '../src/journal/serialize'
import { deriveUuid } from '../src/orchestrator/context'
import type { StepRecord } from '../src/types'

const rec = (seq: number, name: string, result: unknown = 'r'): StepRecord => ({
  seq,
  name,
  result,
  ts: new Date(0),
})

const makeJob = (over: Partial<JournalableJob> = {}): JournalableJob => ({
  id: 'job1',
  status: 'active',
  claimToken: 'tok',
  steps: [],
  journalBytes: 0,
  ...over,
})

describe('serialize: assertSerializable', () => {
  it('accepts primitives, null, Date, plain objects, arrays', () => {
    expect(() =>
      assertSerializable(
        { a: 1, b: 'x', c: true, d: null, e: new Date(), f: [1, { g: 2 }] },
        's',
      ),
    ).not.toThrow()
  })

  it('rejects functions', () => {
    expect(() => assertSerializable({ fn: () => 1 }, 's')).toThrow(
      NonSerializableStepResult,
    )
  })

  it('rejects class instances', () => {
    class Foo {
      x = 1
    }
    expect(() => assertSerializable(new Foo(), 's')).toThrow(
      NonSerializableStepResult,
    )
  })

  it('rejects bigint and symbol', () => {
    expect(() => assertSerializable({ b: 10n }, 's')).toThrow(
      NonSerializableStepResult,
    )
    expect(() => assertSerializable({ s: Symbol('x') }, 's')).toThrow(
      NonSerializableStepResult,
    )
  })

  it('rejects undefined-valued object keys', () => {
    expect(() => assertSerializable({ a: undefined }, 's')).toThrow(
      NonSerializableStepResult,
    )
  })

  it('rejects cycles', () => {
    const o: Record<string, unknown> = {}
    o.self = o
    expect(() => assertSerializable(o, 's')).toThrow(NonSerializableStepResult)
  })

  it('names the step in the error', () => {
    expect(() => assertSerializable({ fn: () => 1 }, 'my-step')).toThrow(
      /my-step/,
    )
  })
})

describe('serialize: void sentinel', () => {
  it('round-trips undefined through stored form', () => {
    const stored = toStored(undefined)
    expect(stored).not.toBeUndefined() // valid BSON
    expect(fromStored(stored)).toBeUndefined()
  })

  it('passes through normal values', () => {
    expect(fromStored(toStored(42))).toBe(42)
    expect(fromStored(toStored({ a: 1 }))).toEqual({ a: 1 })
  })
})

describe('serialize: size guard', () => {
  it('approxRecordBytes grows with record size', () => {
    const small = approxRecordBytes(rec(0, 'a', 'x'))
    const big = approxRecordBytes(rec(0, 'a', 'x'.repeat(1000)))
    expect(big).toBeGreaterThan(small)
  })

  it('guardAppend returns the incoming record size for the running total', () => {
    const r = rec(0, 'a', 'x')
    expect(guardAppend(r, 0, 1e6)).toBe(approxRecordBytes(r))
  })

  it('guardAppend counts the running journal total, not just the record', () => {
    const small = rec(1, 'b', 'y')
    expect(() => guardAppend(small, 0, 1000)).not.toThrow()
    // The same small record over a nearly-full journal must trip the cap.
    expect(() => guardAppend(small, 999, 1000)).toThrow(JournalTooLarge)
  })

  it('guardAppend throws JournalTooLarge over the soft cap', () => {
    const huge = rec(0, 'big', 'x'.repeat(5000))
    expect(() => guardAppend(huge, 0, 1000)).toThrow(JournalTooLarge)
  })

  it('guardAppend names the offending step', () => {
    const huge = rec(0, 'fat-step', 'x'.repeat(5000))
    expect(() => guardAppend(huge, 0, 1000)).toThrow(/fat-step/)
  })
})

describe('inMemory: appendStep fencing', () => {
  it('appends when active and token matches, and bumps the running total', () => {
    const job = makeJob()
    expect(appendStepInMemory(job, 'tok', rec(0, 'a'), 1e6)).toEqual({
      status: 'appended',
    })
    expect(job.steps).toHaveLength(1)
    expect(job.journalBytes).toBeGreaterThan(0)
  })

  it('lease-lost on wrong token', () => {
    const job = makeJob()
    expect(appendStepInMemory(job, 'WRONG', rec(0, 'a'), 1e6)).toEqual({
      status: 'lease-lost',
    })
    expect(job.steps).toHaveLength(0)
  })

  it('lease-lost when not active', () => {
    const job = makeJob({ status: 'pending' })
    expect(appendStepInMemory(job, 'tok', rec(0, 'a'), 1e6)).toEqual({
      status: 'lease-lost',
    })
  })

  it('lease-lost holds even when attempt would collide (token-fenced)', () => {
    // The fence is on claimToken, not attempt — a stale worker with a colliding
    // attempt number but the old token must still be rejected.
    const job = makeJob({ claimToken: 'new-token' })
    expect(appendStepInMemory(job, 'old-token', rec(0, 'a'), 1e6)).toEqual({
      status: 'lease-lost',
    })
  })

  it('already-recorded on same seq + same name carries the existing record', () => {
    const job = makeJob({ steps: [rec(0, 'a', 'stored')] })
    expect(appendStepInMemory(job, 'tok', rec(0, 'a', 'new'), 1e6)).toEqual({
      status: 'already-recorded',
      existing: rec(0, 'a', 'stored'),
    })
    // does not append a duplicate, keeps the stored value
    expect(job.steps).toHaveLength(1)
    expect(job.steps[0].result).toBe('stored')
  })

  it('NondeterminismError on same seq + different name', () => {
    const job = makeJob({ steps: [rec(0, 'a')] })
    expect(() => appendStepInMemory(job, 'tok', rec(0, 'b'), 1e6)).toThrow(
      NondeterminismError,
    )
  })

  it('throws JournalTooLarge before appending', () => {
    const job = makeJob()
    const huge = rec(0, 'big', 'x'.repeat(5000))
    expect(() => appendStepInMemory(job, 'tok', huge, 1000)).toThrow(
      JournalTooLarge,
    )
    expect(job.steps).toHaveLength(0)
  })
})

describe('inMemory: complete/heartbeat fencing', () => {
  it('completeClaimed succeeds with the right token and runs onComplete', () => {
    const job = makeJob()
    let done = false
    expect(
      completeClaimedInMemory(job, 'tok', () => {
        done = true
      }),
    ).toBe('completed')
    expect(done).toBe(true)
  })

  it('completeClaimed lease-lost with wrong token, onComplete not run', () => {
    const job = makeJob()
    let done = false
    expect(
      completeClaimedInMemory(job, 'WRONG', () => {
        done = true
      }),
    ).toBe('lease-lost')
    expect(done).toBe(false)
  })

  it('heartbeatClaimed fences on token', () => {
    const job = makeJob()
    let beat = 0
    expect(
      heartbeatClaimedInMemory(job, 'tok', () => {
        beat++
      }),
    ).toBe('heartbeated')
    expect(
      heartbeatClaimedInMemory(job, 'WRONG', () => {
        beat++
      }),
    ).toBe('lease-lost')
    expect(beat).toBe(1)
  })
})

describe('context: deriveUuid determinism (bootstrap seed)', () => {
  const seed = 'a'.repeat(32)

  it('uuid(label) is deterministic per label and distinct across labels', () => {
    expect(deriveUuid(seed, 'a')).toBe(deriveUuid(seed, 'a'))
    expect(deriveUuid(seed, 'a')).not.toBe(deriveUuid(seed, 'b'))
  })

  it('uuid is distinct across seeds (jobs) for the same label', () => {
    const other = 'b'.repeat(32)
    expect(deriveUuid(seed, 'a')).not.toBe(deriveUuid(other, 'a'))
  })

  it('uuid is UUID-shaped and returns a string (not a Promise)', () => {
    const u = deriveUuid(seed, 'a')
    expect(typeof u).toBe('string')
    expect(u).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    )
  })
})

describe('assertStepMatches (shared divergence predicate)', () => {
  /**
   * The rule was coded three times — on read in octx.step(), and at append time
   * in both the in-memory journal and the Mongo adapter. Three copies of one
   * invariant is three chances to drift, and a divergence check that disagrees
   * with itself is worse than one that is merely strict. Pure, so it is testable
   * without Mongo.
   */
  it('passes when the recorded name matches', () => {
    expect(() => assertStepMatches('job-1', 3, 'charge', 'charge')).not.toThrow()
  })

  it('throws NondeterminismError when the names differ', () => {
    expect(() => assertStepMatches('job-1', 3, 'charge', 'refund')).toThrow(
      NondeterminismError,
    )
  })

  it('reports the job, seq and both names so the divergence is diagnosable', () => {
    try {
      assertStepMatches('job-42', 7, 'recorded-step', 'expected-step')
      expect.unreachable('should have thrown')
    } catch (err) {
      const e = err as NondeterminismError & { jobId?: string; seq?: number }
      expect(e).toBeInstanceOf(NondeterminismError)
      expect(e.message).toContain('7')
      expect(e.message).toContain('recorded-step')
      expect(e.message).toContain('expected-step')
    }
  })

  it('treats names differing only by case as divergence', () => {
    // Step names are identifiers, not prose: 'Charge' is not 'charge'.
    expect(() => assertStepMatches('job-1', 0, 'Charge', 'charge')).toThrow(
      NondeterminismError,
    )
  })
})
