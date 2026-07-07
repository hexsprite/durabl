/** MongoJobQueue - MongoDB-backed durable job queue. */
import { randomUUID } from 'node:crypto'

import type { Collection, Db } from 'mongodb'

import { defaultLogger, type Logger } from '../logger'
import { JournalTooLarge, NondeterminismError } from '../journal/errors'
import {
  approxRecordBytes,
  DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
  guardAppend,
  sortBySeq,
} from '../journal/serialize'
import {
  type AppendStepResult,
  type CompleteClaimedResult,
  type DedupeScope,
  type EnqueueOptions,
  type HeartbeatClaimedResult,
  type Job,
  type JobDoc,
  jobDocToJob,
  type JobHandle,
  type JobStatus,
  type LifecycleWriteResult,
  type QueueStats,
  type StepRecord,
} from '../types'

import { retryBackoffMs } from './backoff'
import type { IJobQueueBackend } from './IJobQueueBackend'
import { MongoChangeStreamWatcher } from './MongoChangeStreamWatcher'
import { createJobIndexes } from './mongoJobIndexes'

export interface MongoJobQueueOptions {
  /** Database handle from a connected `MongoClient`. */
  db: Db
  /** Collection name for job documents. Default: `'jobs'`. */
  collectionName?: string
  /**
   * Enable push notifications via MongoDB change streams (requires a
   * replica set). Default: `false` (poll-only).
   */
  useChangeStreams?: boolean
  /** Injectable logger. Default: console. */
  logger?: Logger
  /**
   * Soft cap (bytes) on the cumulative step journal + logs before `appendStep`
   * throws `JournalTooLarge`. Default: 8MB (well under Mongo's 16MB cap). §8.1.
   */
  journalSoftLimitBytes?: number
}

type JobAvailableListener = (type: string) => void

/**
 * Does this driver/server error mean "the resulting document exceeds the BSON
 * size cap"? Matched robustly across server/driver versions. Observed in the
 * wild (mongod 7.x via node driver): `MongoServerError` code 10334 with
 * message "BSONObj size: <n> is invalid. Size must be between 0 and
 * 16793600(16MB)". Other versions use codeName `BSONObjectTooLarge`,
 * "Resulting document after update is larger than 16777216", or the
 * client-side "object to insert too large".
 */
function isBsonTooLargeError(err: unknown): boolean {
  if (!(err instanceof Error)) return false
  const { code, codeName } = err as { code?: unknown; codeName?: unknown }
  return (
    code === 10334 ||
    codeName === 'BSONObjectTooLarge' ||
    /BSONObjectTooLarge|BSONObj size:.*is invalid|object to insert too large|larger than (?:the )?maximum size|Resulting document after update is larger/i.test(
      err.message,
    )
  )
}

export class MongoJobQueue implements IJobQueueBackend {
  private db: Db
  private collection: Collection<JobDoc>
  private useChangeStreams: boolean
  private logger: Logger
  private journalSoftLimitBytes: number
  private watcher: MongoChangeStreamWatcher | null = null
  /** Buffer listeners subscribed before startup() so they get attached. */
  private pendingListeners: Set<JobAvailableListener> = new Set()
  /** Prevents startup() from assigning a watcher after shutdown() has run. */
  private shuttingDown = false

  constructor(options: MongoJobQueueOptions) {
    this.db = options.db
    this.collection = options.db.collection<JobDoc>(
      options.collectionName ?? 'jobs',
    )
    this.useChangeStreams = options.useChangeStreams ?? false
    this.journalSoftLimitBytes =
      options.journalSoftLimitBytes ?? DEFAULT_JOURNAL_SOFT_LIMIT_BYTES
    this.logger = (options.logger ?? defaultLogger).child({
      category: 'MongoJobQueue',
    })
  }

  async startup(): Promise<void> {
    await createJobIndexes(this.collection)
    if (!this.useChangeStreams) return
    const watcher = new MongoChangeStreamWatcher(
      this.collection,
      this.db,
      this.logger,
    )
    // Attach before start() so events racing the stream open are captured.
    for (const listener of this.pendingListeners) watcher.addListener(listener)
    await watcher.start() // throws on non-replica-set; callers degrade to poll
    // Guard: shutdown() may have run while we were awaiting start(). If so,
    // clean up the just-opened stream and bail — don't assign this.watcher.
    if (this.shuttingDown) {
      await watcher.stop()
      return
    }
    // Re-flush listeners added during the await. Safe: watcher.listeners is a
    // Set, so duplicate adds from the pre-flush are silently deduplicated.
    for (const listener of this.pendingListeners) watcher.addListener(listener)
    this.pendingListeners.clear()
    this.watcher = watcher
  }

  /** Live state (not config): `false` if startup() threw or shutdown() ran.
   * @internal Test-only — not part of the public IJobQueueBackend contract. */
  isChangeStreamsActive(): boolean {
    return this.watcher !== null
  }

  /** Subscribe to push notifications. Returns `null` when disabled. Pre-startup
   * listeners are buffered and flushed in startup(). Closure cleans up both. */
  onJobAvailable(listener: JobAvailableListener): (() => void) | null {
    if (!this.useChangeStreams || this.shuttingDown) return null
    if (this.watcher) return this.watcher.addListener(listener)
    this.pendingListeners.add(listener)
    return () => {
      this.pendingListeners.delete(listener)
      this.watcher?.removeListener(listener)
    }
  }

  async shutdown(_timeoutMs?: number): Promise<void> {
    this.shuttingDown = true
    if (this.watcher) {
      await this.watcher.stop()
      this.watcher = null
    }
    this.pendingListeners.clear() // drop any never-attached listeners
  }

  async enqueue(
    type: string,
    data: unknown,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    const now = new Date()
    const dedupeScope: DedupeScope = options.dedupeScope ?? 'pending+active'
    const runAt = options.delay ? new Date(now.getTime() + options.delay) : now

    const doc: JobDoc = {
      _id: randomUUID(),
      type,
      data,
      status: 'pending',
      priority: options.priority ?? 0,
      attempt: 0,
      maxAttempts: options.maxAttempts ?? 3,
      dedupeKey: options.dedupeKey,
      dedupeScope: options.dedupeKey ? dedupeScope : undefined,
      backoff: options.backoff,
      backoffDelay: options.backoffDelay,
      backoffMaxDelay: options.backoffMaxDelay,
      runAt,
      createdAt: now,
      logs: [],
      journalBytes: 0,
    }

    try {
      await this.collection.insertOne(doc)
      return doc._id
    } catch (err) {
      if (this.isDuplicateKeyError(err)) return null
      throw err
    }
  }

  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options: EnqueueOptions = {},
  ): Promise<JobHandle<T> | null> {
    const now = new Date()
    const dedupeScope: DedupeScope = options.dedupeScope ?? 'pending+active'

    // Coalescing check - if pending job exists, return null
    if (options.dedupeKey) {
      const pending = await this.collection.findOne({
        dedupeKey: options.dedupeKey,
        status: 'pending',
      })
      if (pending) return null
    }

    const doc: JobDoc<T> = {
      _id: randomUUID(),
      type,
      data,
      status: 'active',
      priority: options.priority ?? 0,
      attempt: 1,
      maxAttempts: options.maxAttempts ?? 3,
      dedupeKey: options.dedupeKey,
      dedupeScope: options.dedupeKey ? dedupeScope : undefined,
      backoff: options.backoff,
      backoffDelay: options.backoffDelay,
      backoffMaxDelay: options.backoffMaxDelay,
      runAt: now,
      createdAt: now,
      claimedAt: now,
      claimToken: randomUUID(),
      logs: [],
      journalBytes: 0,
    }

    try {
      await this.collection.insertOne(doc as JobDoc)
      return this.createHandle(doc._id, data, doc.claimToken)
    } catch (err) {
      if (this.isDuplicateKeyError(err)) return null
      throw err
    }
  }

  async claimNext<T>(type: string): Promise<Job<T> | null> {
    const now = new Date()
    const doc = await this.collection.findOneAndUpdate(
      { type, status: 'pending', runAt: { $lte: now } },
      {
        // Mint a fresh per-claim lease nonce. All orchestration fencing keys on
        // this, not on `attempt` (R8) — an admin/manual re-activation that does
        // not bump `attempt` would otherwise silently break the fence.
        $set: {
          status: 'active' as JobStatus,
          claimedAt: now,
          claimToken: randomUUID(),
        },
        $inc: { attempt: 1 },
      },
      { sort: { priority: 1, runAt: 1 }, returnDocument: 'after' },
    )
    // includeClaimToken: the claim path is the one reader allowed to see the
    // live fencing token (processJob fences lifecycle writes with it).
    return doc ? jobDocToJob(doc as JobDoc<T>, true) : null
  }

  /**
   * `$push` + `$inc` fragment for one log entry. Every log write bumps
   * `journalBytes` so the §8.1 running total stays exact for the size guard.
   */
  private logWrite(
    message: string,
    timestamp = new Date(),
  ): Record<string, unknown> {
    const entry = { timestamp, message }
    return {
      $push: { logs: entry },
      $inc: { journalBytes: approxRecordBytes(entry) },
    }
  }

  /** Lifecycle filter: fenced (`active` + matching token) when a token is
   * given, plain `_id` lookup otherwise. */
  private lifecycleFilter(
    jobId: string,
    claimToken?: string,
  ): Record<string, unknown> {
    return claimToken
      ? { _id: jobId, status: 'active' as JobStatus, claimToken }
      : { _id: jobId }
  }

  async complete(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const res = await this.collection.updateOne(
      this.lifecycleFilter(jobId, claimToken),
      {
        $set: { status: 'completed' as JobStatus, completedAt: new Date() },
      },
    )
    return claimToken && res.matchedCount !== 1 ? 'lease-lost' : 'applied'
  }

  async fail(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const filter = this.lifecycleFilter(jobId, claimToken)
    const job = await this.collection.findOne(filter)
    if (!job) return claimToken ? 'lease-lost' : 'applied'

    const now = new Date()
    const exhausted = job.attempt >= job.maxAttempts

    let matchedCount: number
    if (exhausted) {
      const res = await this.collection.updateOne(filter, {
        $set: {
          status: 'failed' as JobStatus,
          failReason: reason,
          failedAt: now,
        },
        ...this.logWrite(`Failed: ${reason}`, now),
      })
      matchedCount = res.matchedCount
    } else {
      // Space the retry: push runAt into the future by a jittered backoff so
      // a fast-failing handler can't burn every attempt in milliseconds and a
      // downstream outage doesn't become an instant-retry storm.
      const runAt = new Date(now.getTime() + retryBackoffMs(job.attempt, job))
      const res = await this.collection.updateOne(filter, {
        $set: { status: 'pending' as JobStatus, failReason: reason, runAt },
        ...this.logWrite(`Attempt failed: ${reason}`, now),
      })
      matchedCount = res.matchedCount
    }
    return claimToken && matchedCount !== 1 ? 'lease-lost' : 'applied'
  }

  async failFatal(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const now = new Date()
    const res = await this.collection.updateOne(
      this.lifecycleFilter(jobId, claimToken),
      {
        $set: {
          status: 'failed' as JobStatus,
          failReason: reason,
          failedAt: now,
        },
        ...this.logWrite(`Fatal: ${reason}`, now),
      },
    )
    return claimToken && res.matchedCount !== 1 ? 'lease-lost' : 'applied'
  }

  async log(jobId: string, message: string): Promise<void> {
    await this.collection.updateOne({ _id: jobId }, this.logWrite(message))
  }

  async heartbeat(jobId: string): Promise<void> {
    await this.collection.updateOne(
      { _id: jobId },
      {
        $set: { claimedAt: new Date() },
      },
    )
  }

  /**
   * Extend the lease on many running jobs in a single write.
   *
   * Equivalent to calling {@link heartbeat} for each id, but collapses N
   * `updateOne` calls into one `updateMany` so periodic keepalive write load
   * is independent of per-instance concurrency. Only `active` jobs are
   * touched — completed/failed/pending docs are left alone.
   *
   * @param jobIds Ids of jobs currently running on this instance.
   */
  async batchHeartbeat(jobIds: string[]): Promise<void> {
    if (jobIds.length === 0) return
    await this.collection.updateMany(
      { _id: { $in: jobIds }, status: 'active' },
      { $set: { claimedAt: new Date() } },
    )
  }

  // --- Durable-orchestration journal capability (§3.6) ----------------------

  async readSteps(jobId: string): Promise<StepRecord[]> {
    const doc = await this.collection.findOne(
      { _id: jobId },
      { projection: { steps: 1 } },
    )
    // Normalize to ascending seq — physical $push order is not execution order
    // under concurrent fan-out (§3.6).
    return sortBySeq(doc?.steps ?? [])
  }

  /**
   * Lease-fenced, idempotent conditional append. The write filters on
   * `{ status: 'active', claimToken, 'steps.seq': { $ne } }` so it is atomic and
   * a stale worker cannot append. Also bumps `claimedAt` (free lease extension,
   * §7.4). Serialization + cumulative-size guards run before the write so the
   * journal is never corrupted and the 16MB cap surfaces as a clear error.
   */
  async appendStep(
    jobId: string,
    claimToken: string,
    record: StepRecord,
  ): Promise<AppendStepResult> {
    // Pre-read is O(1) on the wire: only the lease fields, the running
    // journalBytes total, and (via $elemMatch) the single same-seq step for
    // idempotency/divergence classification — never the full steps/logs arrays.
    const doc = await this.collection.findOne(
      { _id: jobId },
      {
        projection: {
          status: 1,
          claimToken: 1,
          journalBytes: 1,
          steps: { $elemMatch: { seq: record.seq } },
        },
      },
    )
    if (!doc || doc.status !== 'active' || doc.claimToken !== claimToken) {
      return { status: 'lease-lost' }
    }
    const existing = doc.steps?.[0]
    if (existing) return this.classifyExistingStep(jobId, existing, record)

    // Throws NonSerializableStepResult / JournalTooLarge before the write.
    const incomingBytes = guardAppend(
      record,
      doc.journalBytes ?? 0,
      this.journalSoftLimitBytes,
    )

    let res
    try {
      res = await this.collection.updateOne(
        {
          _id: jobId,
          status: 'active',
          claimToken,
          'steps.seq': { $ne: record.seq },
        },
        {
          $push: { steps: record },
          $set: { claimedAt: new Date() },
          $inc: { journalBytes: incomingBytes },
        },
      )
    } catch (err) {
      // The soft cap above checks a PRE-write snapshot, so concurrent fan-out
      // appends sharing one claim token can each pass it and still blow Mongo's
      // 16MB document cap at write time. Surface the documented typed error
      // (§8.1) instead of a raw MongoServerError.
      if (isBsonTooLargeError(err)) {
        throw new JournalTooLarge(
          record.name,
          (doc.journalBytes ?? 0) + incomingBytes,
        )
      }
      throw err
    }
    if (res.modifiedCount === 1) return { status: 'appended' }

    // No write: either the lease was lost in the window or a concurrent append
    // landed this seq first. Re-read to classify precisely.
    const after = await this.collection.findOne(
      { _id: jobId },
      {
        projection: {
          status: 1,
          claimToken: 1,
          steps: { $elemMatch: { seq: record.seq } },
        },
      },
    )
    if (!after || after.status !== 'active' || after.claimToken !== claimToken) {
      return { status: 'lease-lost' }
    }
    const dup = after.steps?.[0]
    if (dup) return this.classifyExistingStep(jobId, dup, record)
    return { status: 'lease-lost' }
  }

  private classifyExistingStep(
    jobId: string,
    existing: StepRecord,
    record: StepRecord,
  ): AppendStepResult {
    if (existing.name !== record.name) {
      throw new NondeterminismError(
        jobId,
        record.seq,
        existing.name,
        record.name,
      )
    }
    return { status: 'already-recorded', existing }
  }

  /**
   * Matches `active` OR `failed` under the same token — NOT active-only. The
   * reaper marks an attempt-exhausted job `failed` without clearing its
   * claimToken; if that worker then finishes every step, completed work must
   * flip the job to `completed` (clearing `failReason`) rather than be lost.
   * A genuinely reclaimed job holds a DIFFERENT token → still `'lease-lost'`.
   */
  async completeClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<CompleteClaimedResult> {
    const res = await this.collection.updateOne(
      {
        _id: jobId,
        claimToken,
        status: { $in: ['active', 'failed'] satisfies JobStatus[] },
      },
      {
        $set: { status: 'completed' as JobStatus, completedAt: new Date() },
        $unset: { failReason: '', failedAt: '' },
      },
    )
    return res.matchedCount === 1 ? 'completed' : 'lease-lost'
  }

  async heartbeatClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<HeartbeatClaimedResult> {
    const res = await this.collection.updateOne(
      { _id: jobId, status: 'active', claimToken },
      { $set: { claimedAt: new Date() } },
    )
    // matchedCount, not modifiedCount: a same-millisecond claimedAt write is a
    // no-op modification but the lease is still held.
    return res.matchedCount === 1 ? 'heartbeated' : 'lease-lost'
  }

  async findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null> {
    const doc = (await this.collection.findOne(query)) as JobDoc<T> | null
    // General read view: the live fencing claimToken is deliberately NOT
    // exposed here — only the claim path (claimNext) may see it.
    return doc ? jobDocToJob(doc) : null
  }

  async getStats(type?: string): Promise<QueueStats> {
    const q = type ? { type } : {}
    const count = (s: JobStatus) =>
      this.collection.countDocuments({ ...q, status: s })
    const [pending, active, completed, failed] = await Promise.all([
      count('pending'),
      count('active'),
      count('completed'),
      count('failed'),
    ])
    return { pending, active, completed, failed }
  }

  /**
   * Recover stuck jobs whose lease has expired (still `active` but
   * `claimedAt` older than the visibility timeout — the worker died or
   * wedged without heartbeating).
   *
   * Each stuck job is routed through the same retry/terminal decision as
   * {@link fail}, NOT blanket-reset to `pending`:
   *
   * - Retries remain (`attempt < maxAttempts`) → back to `pending` with
   *   `runAt` pushed into the future by a jittered backoff. Without the
   *   backoff a handler that wedges the worker would stall → be recovered →
   *   re-claimed → wedge again *immediately*, pegging CPU/Mongo every
   *   visibility window (hot retry loop). The future `runAt` breaks the loop.
   * - Retries exhausted (`attempt >= maxAttempts`) → terminal `failed`.
   *   The old code resurrected these forever because it never checked the
   *   cap.
   *
   * Recovery does not bump `attempt` — the subsequent re-claim does that, so
   * the count stays accurate.
   *
   * Prefer driving this via `JobQueue.startReaper()`, which schedules it with
   * the queue's configured `visibilityTimeoutMs` — the single source of truth
   * the Orchestrator also sizes heartbeats from (§7.1). Passing a custom
   * value here is for tests/manual ops only; a value that disagrees with the
   * queue's breaks the heartbeat/lease contract.
   *
   * @returns Number of stuck jobs handled (re-queued + failed).
   */
  async recoverStuckJobs(visibilityTimeoutMs = 300000): Promise<number> {
    const cutoff = new Date(Date.now() - visibilityTimeoutMs)
    const cursor = this.collection.find({
      status: 'active',
      claimedAt: { $lt: cutoff },
    })

    let handled = 0
    for await (const job of cursor) {
      const now = new Date()
      const exhausted = job.attempt >= job.maxAttempts

      if (exhausted) {
        await this.collection.updateOne(
          { _id: job._id, status: 'active' },
          {
            $set: {
              status: 'failed' as JobStatus,
              failReason: 'Stalled — retries exhausted',
              failedAt: now,
            },
            ...this.logWrite('Stalled — retries exhausted', now),
          },
        )
      } else {
        const runAt = new Date(now.getTime() + retryBackoffMs(job.attempt, job))
        await this.collection.updateOne(
          { _id: job._id, status: 'active' },
          {
            $set: { status: 'pending' as JobStatus, runAt },
            ...this.logWrite('Recovered (stalled)', now),
          },
        )
      }
      handled++
    }
    return handled
  }

  /** Clean up old completed/failed jobs. Default: 7 days. Returns count removed. */
  async cleanupOldJobs(maxAgeMs = 7 * 24 * 60 * 60 * 1000): Promise<number> {
    const cutoff = new Date(Date.now() - maxAgeMs)
    const result = await this.collection.deleteMany({
      status: { $in: ['completed', 'failed'] },
      $or: [{ completedAt: { $lt: cutoff } }, { failedAt: { $lt: cutoff } }],
    })
    return result.deletedCount
  }

  getCollection(): Collection<JobDoc> {
    return this.collection
  }

  async resetStorage(): Promise<void> {
    await this.collection.deleteMany({})
  }

  private createHandle<T>(
    jobId: string,
    data: T,
    claimToken?: string,
  ): JobHandle<T> {
    return {
      id: jobId,
      data,
      complete: async () => {
        await this.complete(jobId, claimToken)
      },
      fail: async (reason: string) => {
        await this.fail(jobId, reason, claimToken)
      },
      log: (msg: string) => {
        void this.log(jobId, msg)
      },
    }
  }

  private isDuplicateKeyError(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false
    const e = err as { code?: number; message?: string }
    return e.code === 11000 || (e.message?.includes('E11000') ?? false)
  }
}
