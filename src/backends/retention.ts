/**
 * Shared retention policy for `cleanupOldJobs`. Every backend resolves its
 * argument here, so all of them validate and default the same way.
 */
import type { RetentionOptions } from '../types'

export const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000
export const DEFAULT_RETENTION_BATCH_SIZE = 1000

export interface ResolvedRetention {
  completedMaxAgeMs: number
  /** `null` means dead letters are kept. */
  deadLetterMaxAgeMs: number | null
  batchSize: number
}

function assertAge(name: string, value: number): void {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new Error(
      `cleanupOldJobs: ${name} must be a finite number >= 0, got ${String(value)}`,
    )
  }
}

/**
 * A number applies to completed jobs and dead letters alike (the original
 * behaviour). An object keeps dead letters unless `deadLetterMaxAgeMs` is set.
 * Throws before any delete when an age or the batch size is invalid.
 */
export function resolveRetention(
  retention: number | RetentionOptions = DEFAULT_RETENTION_MS,
): ResolvedRetention {
  if (typeof retention === 'number') {
    assertAge('maxAgeMs', retention)
    return {
      completedMaxAgeMs: retention,
      deadLetterMaxAgeMs: retention,
      batchSize: DEFAULT_RETENTION_BATCH_SIZE,
    }
  }
  const completedMaxAgeMs = retention.completedMaxAgeMs ?? DEFAULT_RETENTION_MS
  assertAge('completedMaxAgeMs', completedMaxAgeMs)
  const deadLetterMaxAgeMs = retention.deadLetterMaxAgeMs ?? null
  if (deadLetterMaxAgeMs !== null) {
    assertAge('deadLetterMaxAgeMs', deadLetterMaxAgeMs)
  }
  const batchSize = retention.batchSize ?? DEFAULT_RETENTION_BATCH_SIZE
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new Error(
      `cleanupOldJobs: batchSize must be a positive integer, got ${String(batchSize)}`,
    )
  }
  return { completedMaxAgeMs, deadLetterMaxAgeMs, batchSize }
}

/** True when a terminal in-memory job is old enough to delete. */
export function isExpired(
  job: { status: string; completedAt?: Date; failedAt?: Date },
  policy: ResolvedRetention,
  nowMs: number,
): boolean {
  if (job.status === 'completed') {
    return (
      job.completedAt !== undefined &&
      job.completedAt.getTime() < nowMs - policy.completedMaxAgeMs
    )
  }
  if (job.status === 'failed' || job.status === 'superseded') {
    return (
      policy.deadLetterMaxAgeMs !== null &&
      job.failedAt !== undefined &&
      job.failedAt.getTime() < nowMs - policy.deadLetterMaxAgeMs
    )
  }
  return false
}
