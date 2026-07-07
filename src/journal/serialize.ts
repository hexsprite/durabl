/**
 * Journal serialization guards (§8) + void-step sentinel.
 *
 * Pure helpers shared by every journal-capable backend so the BSON-serializable
 * constraint and the cumulative-size cap are enforced at the write boundary —
 * and therefore caught in unit tests against the in-memory backends, with no
 * Mongo required.
 */
import type { StepRecord } from '../types'

import { JournalTooLarge, NonSerializableStepResult } from './errors'

/** Default cumulative-journal soft cap, well under Mongo's 16MB BSON limit. */
export const DEFAULT_JOURNAL_SOFT_LIMIT_BYTES = 8_000_000

/**
 * Stored in place of a `step<void>`/`undefined` result so the journal records
 * "this step completed" without storing a literal `undefined` (invalid BSON in
 * an object position). Replayed back to `undefined` by {@link fromStored}.
 */
const VOID_SENTINEL = { __durablVoid: true } as const

/** Map a step's raw return value to its stored form (void → sentinel). */
export function toStored(value: unknown): unknown {
  return value === undefined ? VOID_SENTINEL : value
}

/** Map a stored value back to its runtime form (sentinel → `undefined`). */
export function fromStored(stored: unknown): unknown {
  if (
    stored !== null &&
    typeof stored === 'object' &&
    (stored as { __durablVoid?: unknown }).__durablVoid === true
  ) {
    return undefined
  }
  return stored
}

/**
 * Throw {@link NonSerializableStepResult} if `value` is not BSON-serializable.
 *
 * Allowed: primitives (string/number/boolean), `null`, `Date`, plain objects,
 * arrays. Rejected: functions, symbols, bigint, class instances, cycles, and
 * `undefined`-valued object keys. (`undefined` arrives only via the void
 * sentinel, never raw, so a raw top-level `undefined` is not possible here.)
 */
export function assertSerializable(value: unknown, step: string): void {
  const seen = new Set<object>()

  const check = (v: unknown, path: string): void => {
    if (v === null) return
    const t = typeof v
    if (t === 'string' || t === 'number' || t === 'boolean') return
    if (t === 'function' || t === 'symbol' || t === 'bigint') {
      throw new NonSerializableStepResult(step, `${t} at ${path}`)
    }
    if (t === 'undefined') {
      throw new NonSerializableStepResult(step, `undefined value at ${path}`)
    }
    // objects
    const obj = v as object
    if (obj instanceof Date) return
    if (seen.has(obj)) {
      throw new NonSerializableStepResult(step, `circular reference at ${path}`)
    }
    seen.add(obj)
    if (Array.isArray(obj)) {
      obj.forEach((e, i) => check(e, `${path}[${i}]`))
      seen.delete(obj)
      return
    }
    const proto = Object.getPrototypeOf(obj)
    if (proto !== Object.prototype && proto !== null) {
      throw new NonSerializableStepResult(
        step,
        `class instance (${(obj.constructor as { name?: string })?.name ?? 'unknown'}) at ${path}`,
      )
    }
    for (const [k, val] of Object.entries(obj)) {
      check(val, `${path}.${k}`)
    }
    seen.delete(obj)
  }

  check(value, '<result>')
}

/**
 * Approximate the serialized byte size of one record (step or log entry).
 * Cheap `JSON.stringify` length — an over- not under-estimate for ASCII, which
 * is the safe direction for a cap.
 */
export function approxRecordBytes(value: unknown): number {
  return JSON.stringify(value)?.length ?? 0
}

/**
 * Run the serialization + size guards for an append against a **running
 * journal-bytes total** (steps + logs share the same 16MB document budget,
 * §8.1). Backends maintain `currentJournalBytes` incrementally so the guard is
 * O(record), not O(journal). Throws {@link NonSerializableStepResult} or
 * {@link JournalTooLarge} on violation.
 *
 * @returns the incoming record's approximate byte size, for the caller to add
 *   to its running total once the append commits.
 */
export function guardAppend(
  record: StepRecord,
  currentJournalBytes: number,
  softLimitBytes: number,
): number {
  assertSerializable(record.result, record.name)
  const incoming = approxRecordBytes(record)
  const total = currentJournalBytes + incoming
  if (total > softLimitBytes) {
    throw new JournalTooLarge(record.name, total)
  }
  return incoming
}

/** Journal read normalization: copy sorted by ascending `seq` (§3.6). */
export function sortBySeq(steps: StepRecord[]): StepRecord[] {
  return [...steps].sort((a, b) => a.seq - b.seq)
}
