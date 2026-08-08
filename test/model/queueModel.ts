/**
 * Reference model for the queue's claim/dedupe/lease semantics.
 *
 * Written for clarity, not performance: it is the executable statement of what
 * the backends are supposed to do, so a generated scenario can be run against
 * both and disagreements reported. Every rule here should be traceable to a
 * documented guarantee — if a rule is in the model but nowhere in the docs, one
 * of the two is wrong.
 *
 * Scope is deliberately the claim path and its invariants, not the whole
 * surface. Journal semantics have their own suites.
 */
import type { DedupeScope, JobStatus } from '../../src/types'

export interface ModelJobSpec {
  type: string
  dedupeKey?: string
  dedupeScope?: DedupeScope
  priority: number
  delayMs: number
  maxAttempts: number
}

export interface ModelJob extends ModelJobSpec {
  id: string
  status: JobStatus
  attempt: number
  runAtMs: number
  /** How many times a handler actually started for this job. */
  executions: number
  /** Terminal writes applied. More than one is a defect. */
  terminalWrites: number
}

/** The operations a generated scenario is built from. */
export type ModelOp =
  | { kind: 'enqueue'; spec: ModelJobSpec }
  | { kind: 'claim'; type: string }
  | { kind: 'complete'; slot: number }
  | { kind: 'fail'; slot: number }
  | { kind: 'reap' }
  | { kind: 'advance'; ms: number }

/**
 * Invariants that must hold after EVERY operation, against any backend.
 *
 * Returned as messages rather than thrown so a property-based run can report
 * all violations for one scenario instead of only the first.
 */
export function checkInvariants(jobs: ModelJob[]): string[] {
  const violations: string[] = []

  // 1. At most one active job per (dedupeKey, dedupeScope). This is what the
  //    `pending` scope promises as a lock replacement, enforced by
  //    `dedupe_active_idx`.
  //
  //    Keyed on the PAIR, not the key alone: uniqueness is enforced by separate
  //    partial indexes per scope, so the same dedupeKey used under two different
  //    scopes is genuinely unguarded. Asserting on the key alone claims a
  //    guarantee durabl does not make. (That laxity is a real footgun and is
  //    tracked on its own bead — but it is not an invariant violation.)
  const activeByKey = new Map<string, number>()
  for (const job of jobs) {
    if (job.status !== 'active' || !job.dedupeKey) continue
    const slot = `${job.dedupeKey}|${job.dedupeScope ?? 'pending+active'}`
    const n = (activeByKey.get(slot) ?? 0) + 1
    activeByKey.set(slot, n)
    if (n > 1) {
      violations.push(
        `two active jobs share ${slot} (mutual exclusion broken)`,
      )
    }
  }

  for (const job of jobs) {
    // 2. attempt must never exceed maxAttempts — the retry budget is a bound,
    //    not a suggestion.
    if (job.attempt > job.maxAttempts) {
      violations.push(
        `job ${job.id} attempt ${job.attempt} exceeds maxAttempts ${job.maxAttempts}`,
      )
    }
    // 3. Exactly-once terminality: nothing reaches a terminal state twice.
    if (job.terminalWrites > 1) {
      violations.push(
        `job ${job.id} received ${job.terminalWrites} terminal writes`,
      )
    }
    // 4. A pending job that is not yet due must not have been claimed.
    if (job.status === 'active' && job.attempt === 0) {
      violations.push(`job ${job.id} is active but was never claimed`)
    }
  }

  return violations
}

/**
 * No job may be silently lost: every enqueued job is either terminal or still
 * legitimately reachable (pending, or active under a live lease).
 *
 * Checked at end of scenario rather than per-op, because mid-scenario a job is
 * routinely in flight.
 */
export function checkNoLostJobs(jobs: ModelJob[], enqueued: number): string[] {
  if (jobs.length !== enqueued) {
    return [`${enqueued} jobs enqueued but ${jobs.length} exist`]
  }
  const stranded = jobs.filter(
    (j) => !['pending', 'active', 'completed', 'failed'].includes(j.status),
  )
  return stranded.map((j) => `job ${j.id} in unknown status ${j.status}`)
}

/**
 * Is this job claimable per the documented rules?
 *
 * Used to assert the backend did not withhold work it should have served — the
 * failure mode behind du-pz9, where one contended key stalled everything of the
 * same type.
 */
export function isClaimable(
  job: ModelJob,
  nowMs: number,
  activeDedupeKeys: Set<string>,
): boolean {
  if (job.status !== 'pending') return false
  if (job.runAtMs > nowMs) return false
  if (job.dedupeKey && activeDedupeKeys.has(job.dedupeKey)) return false
  return true
}

/** dedupeKeys currently held by an active job. */
export function activeKeys(jobs: ModelJob[]): Set<string> {
  return new Set(
    jobs
      .filter((j) => j.status === 'active' && j.dedupeKey)
      .map((j) => j.dedupeKey as string),
  )
}
