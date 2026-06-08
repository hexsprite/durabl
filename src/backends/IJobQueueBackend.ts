/**
 * IJobQueueBackend Interface
 *
 * Defines the contract for job queue implementations.
 * Implementations: MongoJobQueue (prod), DummyBackend (unit tests),
 * ImmediateBackend (integration tests)
 */

import type { EnqueueOptions, Job, JobHandle, QueueStats } from '../types'

export interface IJobQueueBackend {
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
   */
  complete(jobId: string): Promise<void>

  /**
   * Mark job as failed. Will retry if attempts remain.
   */
  fail(jobId: string, reason: string): Promise<void>

  /**
   * Mark job as permanently failed. No retry.
   */
  failFatal(jobId: string, reason: string): Promise<void>

  /**
   * Add a log entry to the job.
   */
  log(jobId: string, message: string): Promise<void>

  /**
   * Update job heartbeat to prevent visibility timeout.
   */
  heartbeat(jobId: string): Promise<void>

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
}
