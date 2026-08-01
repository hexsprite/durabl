/**
 * IJobQueueBackend Interface
 *
 * Defines the contract for job queue implementations.
 * Implementations: MongoJobQueue (prod), DummyBackend (unit tests),
 * ImmediateBackend (integration tests)
 */

import type {
  AppendStepResult,
  CompleteClaimedResult,
  EnqueueOptions,
  HeartbeatClaimedResult,
  Job,
  JobHandle,
  LifecycleWriteResult,
  QueueStats,
  StepRecord,
} from '../types'

export interface IJobQueueBackend {
  /**
   * True for backends that execute enqueued jobs *inline* through their own
   * handler registry (see `ImmediateBackend.registerHandler`) instead of the
   * {@link JobQueue.process} poll loop. An {@link Orchestrator} registers its
   * durable wrapper via `queue.process()`, which such a backend never invokes —
   * so an orchestration would silently sit `active` forever. `JobQueue` reads
   * this flag to refuse orchestration construction against an inline backend.
   * Detect-by-presence (absent/false = processor-driven, orchestration-capable).
   */
  readonly executesInline?: boolean

  /**
   * Add a job to the queue.
   * @returns Job ID if created, null if dedupe prevented creation
   */
  enqueue(
    type: string,
    data: unknown,
    options?: EnqueueOptions,
  ): Promise<string | null>

  /**
   * Atomically create and claim a job for immediate inline execution.
   * Used for the coalescing pattern (distributed-lock replacement).
   * @returns JobHandle if claimed, null if job already exists
   */
  claimOrEnqueue<T>(
    type: string,
    data: T,
    options?: EnqueueOptions,
  ): Promise<JobHandle<T> | null>

  /**
   * Claim the next available job for processing.
   * Used by the poll-based processor loop.
   * @returns Job if one was claimed, null if none available
   */
  claimNext<T>(type: string): Promise<Job<T> | null>

  /**
   * Mark job as successfully completed.
   *
   * When `claimToken` is provided the write is lease-fenced: it only applies
   * if the job is still `active` under that token. A miss returns
   * `'lease-lost'` and modifies nothing — a zombie worker can never clobber a
   * job reclaimed by another worker.
   */
  complete(jobId: string, claimToken?: string): Promise<LifecycleWriteResult>

  /**
   * Mark job as failed. Will retry if attempts remain.
   *
   * When `claimToken` is provided the write is lease-fenced (see
   * {@link complete}); a miss returns `'lease-lost'` and modifies nothing.
   */
  fail(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult>

  /**
   * Mark job as permanently failed. No retry.
   *
   * When `claimToken` is provided the write is lease-fenced (see
   * {@link complete}); a miss returns `'lease-lost'` and modifies nothing.
   */
  failFatal(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult>

  /**
   * Add a log entry to the job.
   */
  log(jobId: string, message: string): Promise<void>

  /**
   * Extend a job's lease so the reaper's visibility timeout does not reclaim
   * it.
   *
   * When `claimToken` is provided the write is lease-fenced exactly like
   * {@link complete}: it only applies while the job is still `active` under
   * that token, and a miss returns `'lease-lost'` having modified nothing.
   * Without the fence a zombie worker keeps renewing a lease it no longer
   * holds — extending the *current* owner's window on its behalf and stamping
   * `claimedAt` onto jobs in states where it is meaningless.
   */
  heartbeat(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult>

  /**
   * Find a job by query. Use for utilities like expiring stale jobs.
   * @returns Job if found, null otherwise
   */
  findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null>

  /**
   * Get queue statistics.
   * @param type Optional job type filter
   */
  getStats(type?: string): Promise<QueueStats>

  /**
   * Initialize backend (create indexes, etc).
   */
  startup(): Promise<void>

  /**
   * Graceful shutdown. Wait for active jobs to complete.
   * @param timeoutMs Maximum time to wait
   */
  shutdown(timeoutMs?: number): Promise<void>

  /**
   * Clear all jobs from the queue. Used for testing.
   */
  resetStorage(): Promise<void>

  /**
   * Reaper sweep: route `active` jobs whose lease expired (claimed longer ago
   * than `visibilityTimeoutMs`) back to `pending`, or to terminal `failed`
   * when attempts are exhausted. Optional — test backends may omit it.
   *
   * Prefer driving this via {@link JobQueue.startReaper}, which passes the
   * queue's configured `visibilityTimeoutMs` — the single source of truth the
   * Orchestrator also sizes heartbeats from. Passing a custom value directly
   * is for tests/manual ops only; a value that disagrees with the queue's
   * breaks the heartbeat/lease contract (§7.1).
   *
   * Implementations should bound the work of a single sweep (`maxPerSweep`)
   * and let the remainder drain across later ticks — after a mass worker death
   * an unbounded pass walks the entire stuck set while the system is already
   * degraded. A return value equal to the bound means there is more waiting.
   *
   * @returns Number of stuck jobs handled (re-queued + failed).
   */
  recoverStuckJobs?(
    visibilityTimeoutMs?: number,
    maxPerSweep?: number,
  ): Promise<number>

  /**
   * Subscribe to a push-style notification when a new pending job becomes
   * available. Backends that support real-time notifications (e.g. MongoDB
   * change streams) invoke the listener with the job type shortly after the
   * job document is inserted.
   *
   * The listener may also be invoked with an **empty string** `''` as a
   * catch-up sentinel — for example, after a MongoDB change stream
   * reconnect, to nudge processors to re-poll for any jobs that landed
   * during the gap. Callers should treat an empty-string type as "try any
   * registered processor" rather than looking up a specific queue.
   *
   * ## Return value semantics
   *
   * There are three distinct states, and callers must distinguish them:
   *
   * 1. **Method omitted entirely** — backend has no concept of push (e.g.
   *    `DummyBackend`). Callers detect this via `backend.onJobAvailable?.`
   *    and fall back to polling with the default interval.
   *
   * 2. **Method present, returns `null`** — backend *could* support push
   *    but it is currently disabled (e.g. `MongoJobQueue` with the
   *    `useChangeStreams` flag off). Callers should fall back to polling,
   *    exactly as if the method were omitted. This case exists so the
   *    backend type can stay stable across runtime configuration changes.
   *
   * 3. **Method present, returns an unsubscribe function** — push is
   *    active. Callers may bump their poll interval to a safety-net value
   *    and rely on the listener for low-latency pickup.
   *
   * Backends without push support may omit this method entirely; callers
   * MUST tolerate its absence and MUST also tolerate a `null` return.
   *
   * @returns An unsubscribe function, or `null` if push is not active.
   */
  onJobAvailable?(listener: (type: string) => void): (() => void) | null

  // ---------------------------------------------------------------------------
  // Durable-orchestration journal capability (optional). Detect-by-presence,
  // mirroring `onJobAvailable?`. A backend either implements all four or none;
  // `JobQueue` asserts the capability before an `Orchestrator` uses it.
  // §3.5/§3.6.
  // ---------------------------------------------------------------------------

  /** Read the step journal for a job, normalized to ascending `seq`. */
  readSteps?(jobId: string): Promise<StepRecord[]>

  /**
   * Lease-fenced, idempotent conditional append of one step record. Also bumps
   * `claimedAt` (a free lease extension between steps, §7.4). An
   * `already-recorded` result carries the existing record so callers need no
   * follow-up journal read. §3.6.
   */
  appendStep?(
    jobId: string,
    claimToken: string,
    record: StepRecord,
  ): Promise<AppendStepResult>

  /**
   * Lease-fenced final completion (claim-token-only). §3.6.
   *
   * Matches `active` OR `failed` under the same token: the reaper may mark an
   * attempt-exhausted job `failed` without clearing its token, and if that
   * worker then finishes every step the completed work must not be lost —
   * the run flips to `completed` (clearing `failReason`). A genuinely
   * reclaimed job carries a DIFFERENT token, so it still returns
   * `'lease-lost'`.
   */
  completeClaimed?(
    jobId: string,
    claimToken: string,
  ): Promise<CompleteClaimedResult>

  /** Lease-fenced heartbeat (claim-token-only). §7. */
  heartbeatClaimed?(
    jobId: string,
    claimToken: string,
  ): Promise<HeartbeatClaimedResult>
}
