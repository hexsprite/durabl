/**
 * In-memory journal operations shared by `DummyBackend` and `ImmediateBackend`.
 *
 * Mirrors `MongoJobQueue`'s durable semantics (lease fencing, idempotent
 * same-seq handling, serialization + size guards) so orchestrator logic is
 * fully unit-testable without Mongo — the project's core invariant.
 */
import type {
  AppendStepResult,
  CompleteClaimedResult,
  HeartbeatClaimedResult,
  JobStatus,
  StepRecord,
} from '../types'

import { NondeterminismError } from './errors'
import { guardAppend } from './serialize'

/** Minimal shape an in-memory backend's job record must expose to the journal. */
export interface JournalableJob {
  id: string
  status: JobStatus
  claimToken?: string
  steps: StepRecord[]
  /** Running approximate byte size of steps + logs (mirrors Mongo's field). */
  journalBytes: number
}

/**
 * Lease check shared by the fenced journal writes: the job exists, holds the
 * caller's token, and is in one of `statuses` (default: `active` only).
 */
export function holdsLease(
  job: JournalableJob | undefined,
  claimToken: string,
  statuses: readonly JobStatus[] = ['active'],
): job is JournalableJob {
  return (
    job !== undefined &&
    statuses.includes(job.status) &&
    job.claimToken === claimToken
  )
}

/** §3.6 conditional, lease-fenced append against an in-memory job. */
export function appendStepInMemory(
  job: JournalableJob | undefined,
  claimToken: string,
  record: StepRecord,
  softLimitBytes: number,
): AppendStepResult {
  if (!holdsLease(job, claimToken)) {
    return { status: 'lease-lost' }
  }
  const existing = job.steps.find((s) => s.seq === record.seq)
  if (existing) {
    if (existing.name !== record.name) {
      throw new NondeterminismError(
        job.id,
        record.seq,
        existing.name,
        record.name,
      )
    }
    return { status: 'already-recorded', existing }
  }
  const incoming = guardAppend(record, job.journalBytes, softLimitBytes)
  job.steps.push(record)
  job.journalBytes += incoming
  return { status: 'appended' }
}

/**
 * §3.6 lease-fenced final completion.
 *
 * Matches `active` OR `failed` under the same token (mirrors
 * `MongoJobQueue.completeClaimed`): the reaper may mark an attempt-exhausted
 * job `failed` without clearing its claimToken, and a worker that then
 * finishes every step must flip the run to `completed` rather than lose the
 * work. A genuinely reclaimed job holds a DIFFERENT token → `'lease-lost'`.
 */
export function completeClaimedInMemory(
  job: JournalableJob | undefined,
  claimToken: string,
  onComplete: () => void,
): CompleteClaimedResult {
  if (!holdsLease(job, claimToken, ['active', 'failed'])) {
    return 'lease-lost'
  }
  onComplete()
  return 'completed'
}

/** §7 lease-fenced heartbeat. */
export function heartbeatClaimedInMemory(
  job: JournalableJob | undefined,
  claimToken: string,
  onBeat: () => void,
): HeartbeatClaimedResult {
  if (!holdsLease(job, claimToken)) {
    return 'lease-lost'
  }
  onBeat()
  return 'heartbeated'
}
