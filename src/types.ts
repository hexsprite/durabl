/**
 * durabl Types
 *
 * Core type definitions for the job queue. Backend-agnostic — these types
 * work with any {@link IJobQueueBackend} implementation.
 */

export type JobStatus =
  | 'pending'
  | 'active'
  | 'completed'
  | 'failed'
  | 'superseded'

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

/** Common result when a terminal write cannot apply to a live claim. */
export type TerminalWriteMissResult =
  | {
      status: 'already-terminal'
      terminalStatus: 'completed' | 'failed' | 'superseded'
    }
  | { status: 'lease-lost' }
  | { status: 'not-found' }

/** Result of marking a job successfully completed. */
export type CompleteJobResult =
  | { status: 'completed' }
  | TerminalWriteMissResult

/** Result of failing a job, with retry behavior based on its attempt count. */
export type FailJobResult =
  | { status: 'retry-scheduled' }
  | { status: 'failed-terminal' }
  | { status: 'superseded' }
  | TerminalWriteMissResult

/** Result of failing a job without allowing another retry. */
export type FailFatalJobResult =
  | { status: 'failed-terminal' }
  | TerminalWriteMissResult

/** Result of a heartbeat lifecycle write. */
export type LifecycleWriteResult = 'applied' | 'lease-lost'

/** Result of releasing a live claim back to the due pending queue. */
export type ReleaseJobResult =
  | { status: 'released' }
  | { status: 'superseded' }
  | TerminalWriteMissResult

/** Options for queue-owned execution of an already claimed job. */
export interface RunClaimedOptions {
  /** Additional same-key jobs to claim after the initial job. Default: 10. */
  maxDrains?: number
}

/** Throw from a handler when retrying the job cannot succeed. */
export class FatalJobError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'FatalJobError'
  }
}

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
 * Active claim with lease-fenced lifecycle operations.
 *
 * The queue owns these operations while it invokes a handler. Handlers receive
 * a plain {@link Job} plus {@link JobContext}, never this handle.
 */
export interface JobHandle<T = unknown> extends Job<T> {
  status: 'active'
  complete(): Promise<CompleteJobResult>
  fail(reason: string): Promise<FailJobResult>
  failFatal(reason: string): Promise<FailFatalJobResult>
  heartbeat(): Promise<LifecycleWriteResult>
  release(): Promise<ReleaseJobResult>
  log(message: string): void
}

/** Context passed to job handlers. The queue owns every lifecycle transition. */
export interface JobContext {
  /** Aborted when shutdown or lease loss cancels this claim. */
  signal: AbortSignal
  /**
   * Add a log entry without making a failed log write fail the job.
   */
  log(message: string): void
}

/**
 * Job handler function signature for process()
 */
export type JobHandler<T> = (
  job: Job<T>,
  ctx: JobContext,
) => void | Promise<void>

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
  /**
   * Replace the payload of an existing pending follower.
   * Only valid with `dedupeKey` and `dedupeScope: 'pending'`.
   */
  coalesce?: 'latest'
}

/** Reject invalid latest-payload coalescing before a backend can mutate storage. */
export function assertValidLatestCoalescing(
  options: EnqueueOptions = {},
): void {
  if (options.coalesce !== 'latest') return
  if (options.dedupeKey && options.dedupeScope === 'pending') return
  throw new Error(
    "coalesce: 'latest' requires dedupeKey and dedupeScope: 'pending'",
  )
}

/**
 * Configuration for process() job handlers
 */
export interface ProcessorConfig {
  /** Number of concurrent jobs to process. Default: 1 */
  concurrency?: number
  /** Milliseconds between poll cycles. Default: 5000 */
  pollInterval?: number
  /** Milliseconds between managed lease heartbeats. Default: visibility / 3. */
  heartbeatIntervalMs?: number
}

/**
 * Queue statistics.
 *
 * Depth alone cannot tell a healthy queue from a stuck one: a backlog of 5 that
 * has been waiting 40 minutes is an incident, a backlog of 5000 draining fast is
 * fine. {@link QueueStats.oldestPendingLagMs} is the signal to alert on.
 */
export interface QueueStats {
  pending: number
  active: number
  completed: number
  failed: number
  superseded: number
  /**
   * `runAt` of the oldest job that is pending **and already due**, or `null`
   * when nothing is waiting.
   *
   * Jobs scheduled for the future are excluded deliberately. A job deliberately
   * delayed until next week is not backlog, and counting it would peg the metric
   * permanently red and make it useless.
   */
  oldestPendingRunAt: Date | null
  /**
   * How far past its `runAt` the oldest due pending job is, in ms. `0` when the
   * queue is empty or nothing is overdue. Floored at 0 so clock skew cannot
   * produce a negative lag.
   */
  oldestPendingLagMs: number
}

/** One lifecycle acknowledgement retained for exact ambiguous-write recovery. */
export type TerminalReceipt =
  | {
      claimToken: string | null
      operation: 'complete'
      result: { status: 'completed' }
    }
  | {
      claimToken: string | null
      operation: 'fail'
      result:
        | { status: 'retry-scheduled' }
        | { status: 'failed-terminal' }
        | { status: 'superseded' }
    }
  | {
      claimToken: string | null
      operation: 'failFatal'
      result: { status: 'failed-terminal' }
    }
  | {
      claimToken: string | null
      operation: 'release'
      result: { status: 'released' } | { status: 'superseded' }
    }

/** Receipts indexed by encoded claim token, then lifecycle operation. */
export type TerminalReceipts = Record<
  string,
  Partial<Record<TerminalReceipt['operation'], TerminalReceipt['result']>>
>

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
  /** Per-claim receipts retained across later claims and terminal writes. */
  terminalReceipts?: TerminalReceipts
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

/** Result of starting the queue's stuck-job reaper. */
export type StartReaperResult =
  | { status: 'started'; recovered: number | null }
  | { status: 'already-running' }

/**
 * A queue lifecycle event, for metrics.
 *
 * Logs are for a human reading an incident; events are for a machine counting.
 * Without this a consumer wanting throughput or failure-rate dashboards has to
 * parse structured logs and match on durabl's internal message strings, which
 * makes those strings a de-facto public API that any refactor silently breaks.
 */
export type JobEvent =
  | { kind: 'claimed'; type: string; jobId: string; attempt: number }
  | {
      kind: 'completed'
      type: string
      jobId: string
      /** Claim to terminal write, ms. */
      durationMs: number
    }
  | {
      kind: 'failed'
      type: string
      jobId: string
      durationMs: number
      attempt: number
      maxAttempts: number
      /** False when attempts remain, so a retry is expected. */
      terminal: boolean
      reason: string
    }
  | {
      kind: 'superseded'
      type: string
      jobId: string
      durationMs: number
      reason: string
    }
  | { kind: 'fail-fatal'; type: string; jobId: string; reason: string }
  | {
      kind: 'lease-lost'
      type: string
      jobId: string
      /** The operation that discovered the fence miss. */
      op: string
    }
  | {
      kind: 'reaper-recovered'
      handled: number
      /** Sweep hit the batch cap, so more work is waiting. */
      saturated: boolean
    }
  | {
      kind: 'reaper-error'
      phase: 'startup' | 'periodic'
      message: string
    }
  | { kind: 'shutdown-released'; type: string; jobId: string }

/** Sink for {@link JobEvent}s. Synchronous and fire-and-forget. */
export type JobEventSink = (event: JobEvent) => void
