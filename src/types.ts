/**
 * durabl Types
 *
 * Core type definitions for the job queue. Backend-agnostic — these types
 * work with any {@link IJobQueueBackend} implementation.
 */

export type JobStatus = 'pending' | 'active' | 'completed' | 'failed'

/**
 * Controls duplicate job prevention behavior.
 * - 'pending+active': Only one job with dedupeKey can exist (pending OR active)
 * - 'pending': Only one PENDING job; allows 1 pending + 1 active (coalescing)
 */
export type DedupeScope = 'pending' | 'pending+active'

/**
 * Job document structure (public view).
 */
export interface Job<T = unknown> {
  id: string
  type: string
  data: T
  status: JobStatus
  attempt: number
  maxAttempts: number
  priority: number
  dedupeKey?: string
  dedupeScope?: DedupeScope
  runAt: Date
  createdAt: Date
  claimedAt?: Date
  completedAt?: Date
  failedAt?: Date
  failReason?: string
}

/**
 * Handle returned by claimOrEnqueue() for inline job execution.
 * Allows caller to complete/fail the job after running their code.
 */
export interface JobHandle<T = unknown> {
  id: string
  data: T
  complete(): Promise<void>
  fail(reason: string): Promise<void>
  log(message: string): void
}

/**
 * Context passed to job handlers in process() callbacks.
 */
export interface JobContext {
  /** Mark job as successfully completed */
  complete(): Promise<void>
  /** Mark job as failed (will retry if attempts remain) */
  fail(reason: string): Promise<void>
  /** Mark job as permanently failed (no retry) */
  failFatal(reason: string): Promise<void>
  /** Add log entry to job */
  log(message: string): void
  /** Update heartbeat timestamp (prevents visibility timeout) */
  heartbeat(): Promise<void>
}

/**
 * Job handler function signature for process()
 */
export type JobHandler<T> = (job: Job<T>, ctx: JobContext) => Promise<void>

/**
 * Options for enqueue() and claimOrEnqueue()
 */
export interface EnqueueOptions {
  /** Lower number = higher priority. Default: 0 */
  priority?: number
  /** Milliseconds to delay before job becomes claimable. Default: 0 */
  delay?: number
  /** Maximum retry attempts. Default: 3 */
  maxAttempts?: number
  /** Unique key for duplicate prevention */
  dedupeKey?: string
  /** Scope for dedupe check. Default: 'pending+active' */
  dedupeScope?: DedupeScope
}

/**
 * Configuration for process() job handlers
 */
export interface ProcessorConfig {
  /** Number of concurrent jobs to process. Default: 1 */
  concurrency?: number
  /** Milliseconds between poll cycles. Default: 5000 */
  pollInterval?: number
}

/**
 * Queue statistics
 */
export interface QueueStats {
  pending: number
  active: number
  completed: number
  failed: number
}

/**
 * Internal job document structure (with MongoDB _id)
 */
export interface JobDoc<T = unknown> {
  _id: string
  type: string
  data: T
  status: JobStatus
  priority: number
  attempt: number
  maxAttempts: number
  dedupeKey?: string
  dedupeScope?: DedupeScope
  runAt: Date
  createdAt: Date
  claimedAt?: Date
  completedAt?: Date
  failedAt?: Date
  failReason?: string
  logs: Array<{ timestamp: Date; message: string }>
}

/**
 * Convert internal JobDoc to public Job interface
 */
export function jobDocToJob<T>(doc: JobDoc<T>): Job<T> {
  return {
    id: doc._id,
    type: doc.type,
    data: doc.data,
    status: doc.status,
    attempt: doc.attempt,
    maxAttempts: doc.maxAttempts,
    priority: doc.priority,
    dedupeKey: doc.dedupeKey,
    dedupeScope: doc.dedupeScope,
    runAt: doc.runAt,
    createdAt: doc.createdAt,
    claimedAt: doc.claimedAt,
    completedAt: doc.completedAt,
    failedAt: doc.failedAt,
    failReason: doc.failReason,
  }
}
