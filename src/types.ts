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
 * Retry spacing strategy.
 * - 'exponential' (default): exponential growth with full jitter.
 * - 'fixed': constant delay every retry.
 */
export type BackoffStrategy = 'exponential' | 'fixed'

/**
 * Serializable backoff configuration persisted on a job document so the retry
 * path can compute delays without the original enqueue call in scope.
 */
export interface BackoffConfig {
  /** Strategy. Default: 'exponential'. */
  backoff?: BackoffStrategy
  /** Base delay in ms (also the floor for exponential). Default: 1000. */
  backoffDelay?: number
  /** Maximum delay in ms (cap). Default: 60000. */
  backoffMaxDelay?: number
}

/**
 * A journaled step result. Append-only, keyed by `seq` (not array position —
 * concurrent fan-out can complete out of order). §3.5/§3.6.
 */
export interface StepRecord {
  /** Sequence assigned synchronously at `octx.step()` call time. */
  seq: number
  /** Step name; asserted against the journal at `seq` for divergence (§6). */
  name: string
  /** BSON-serializable result (void steps store an internal sentinel). §8. */
  result: unknown
  ts: Date
}

/**
 * Result of a lifecycle write (`complete`/`fail`/`failFatal`). When the caller
 * supplies a `claimToken`, the write is fenced: a stale token (job reclaimed by
 * another worker) yields `'lease-lost'` and the job is NOT modified. Unfenced
 * calls (no token) always report `'applied'`.
 */
export type LifecycleWriteResult = 'applied' | 'lease-lost'

/**
 * Result of a lease-fenced conditional step append. §3.6.
 *
 * `already-recorded` carries the `existing` journal record so callers (the
 * orchestrator's driver-retry ambiguity path) can return the stored result
 * without re-reading the whole journal.
 */
export type AppendStepResult =
  | { status: 'appended' }
  | { status: 'already-recorded'; existing: StepRecord }
  | { status: 'lease-lost' }
/** Result of a lease-fenced final completion. §3.6. */
export type CompleteClaimedResult = 'completed' | 'lease-lost'
/** Result of a lease-fenced heartbeat. §7. */
export type HeartbeatClaimedResult = 'heartbeated' | 'lease-lost'

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
  /**
   * Per-claim lease nonce minted on every claim. Orchestrator fencing
   * (`appendStep`/`completeClaimed`/`heartbeatClaimed`) filters on this so a
   * stale worker can never mutate a reclaimed job. Absent on backends that
   * don't mint one. §3.6 (R8).
   */
  claimToken?: string
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
export interface EnqueueOptions extends BackoffConfig {
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
  backoff?: BackoffStrategy
  backoffDelay?: number
  backoffMaxDelay?: number
  runAt: Date
  createdAt: Date
  claimedAt?: Date
  completedAt?: Date
  failedAt?: Date
  failReason?: string
  logs: Array<{ timestamp: Date; message: string }>
  /** Per-claim lease nonce (see {@link Job.claimToken}). */
  claimToken?: string
  /** Embedded step journal for durable orchestration. §3.2. */
  steps?: StepRecord[]
  /**
   * Running approximate byte size of `steps` + `logs`, maintained via `$inc`
   * on every append/log write so the §8.1 size guard is O(record) instead of
   * re-stringifying the whole journal. Missing (pre-existing docs) means 0.
   */
  journalBytes?: number
}

/**
 * Convert internal JobDoc to public Job interface.
 *
 * @param includeClaimToken Only the claim path (`claimNext` → `processJob`)
 *   should see the live fencing token; general reads (`findOne`) must not
 *   expose it, or any reader could forge lease-fenced writes. Default: false.
 */
export function jobDocToJob<T>(
  doc: JobDoc<T>,
  includeClaimToken = false,
): Job<T> {
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
    ...(includeClaimToken ? { claimToken: doc.claimToken } : {}),
  }
}
