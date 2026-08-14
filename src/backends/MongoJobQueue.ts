/** MongoJobQueue - MongoDB-backed durable job queue. */
import { randomUUID } from 'node:crypto'

import type { Collection, Db, Filter } from 'mongodb'

import { defaultLogger, type Logger } from '../logger'
import { JournalTooLarge, NondeterminismError } from '../journal/errors'
import {
  DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
  DEFAULT_MAX_LOG_ENTRIES,
  DEFAULT_MAX_LOG_MESSAGE_BYTES,
  guardAppend,
  sortBySeq,
  truncateLogMessage,
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
  type LeaseAwareJobHandle,
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
   * Soft cap (bytes) on the cumulative step journal before `appendStep` throws
   * `JournalTooLarge`. Default: 8MB (well under Mongo's 16MB cap). §8.1.
   * `logs[]` is bounded separately — see `maxLogEntries`.
   */
  journalSoftLimitBytes?: number
  /**
   * Newest-N `logs[]` entries retained per job; older entries are dropped.
   * Bounding the array is what keeps terminal writes (which append a log line)
   * applicable no matter how chatty a handler is. Default: 1000.
   */
  maxLogEntries?: number
  /** Ceiling on one log message; longer messages are clipped. Default: 4000. */
  maxLogMessageBytes?: number
}

type JobAvailableListener = (type: string) => void

/**
 * Stuck jobs handled per {@link MongoJobQueue.recoverStuckJobs} sweep. Bounds
 * how long one pass can run after a mass worker death; the remainder is picked
 * up on the next reaper tick.
 */
const DEFAULT_REAPER_BATCH_SIZE = 1000

interface DedupeIdentity {
  dedupeKey: string
  dedupeScope: DedupeScope
}

/**
 * The dedupe identity a duplicate-key error was raised on, or `null` if this
 * is not a dedupe-index violation and therefore not ours to swallow.
 *
 * Reads the driver's structured `keyValue` rather than parsing the message —
 * message text is not a stable interface, and a partial parse would silently
 * turn a real error into a skipped candidate. A duplicate key on any other
 * index is a genuine fault and must propagate.
 */
function dedupeIdentityFromDuplicateError(
  err: unknown,
): DedupeIdentity | null {
  if (!err || typeof err !== 'object') return null
  const e = err as {
    code?: unknown
    keyValue?: Record<string, unknown>
    keyPattern?: Record<string, unknown>
  }
  if (e.code !== 11000) return null
  const dedupeKey = e.keyValue?.dedupeKey
  const dedupeScope = e.keyValue?.dedupeScope
  if (
    typeof dedupeKey !== 'string' ||
    (dedupeScope !== 'pending' && dedupeScope !== 'pending+active')
  ) {
    return null
  }
  if (
    e.keyPattern &&
    (!('dedupeKey' in e.keyPattern) || !('dedupeScope' in e.keyPattern))
  ) {
    return null
  }
  return { dedupeKey, dedupeScope }
}

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

export class MongoJobQueue implements IJobQueueBackend<LeaseAwareJobHandle> {
  private db: Db
  private collection: Collection<JobDoc>
  private useChangeStreams: boolean
  private logger: Logger
  private journalSoftLimitBytes: number
  private maxLogEntries: number
  private maxLogMessageBytes: number
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
    this.maxLogEntries = options.maxLogEntries ?? DEFAULT_MAX_LOG_ENTRIES
    this.maxLogMessageBytes =
      options.maxLogMessageBytes ?? DEFAULT_MAX_LOG_MESSAGE_BYTES
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

  /**
   * Create+claim a job for inline execution.
   *
   * Mutual exclusion is enforced by the unique partial indexes, never by the
   * pre-read below — that read is a cheap short-circuit only, and being a
   * check-then-act it cannot be the guarantee. Under `dedupeScope: 'pending'`
   * the `dedupe_active_idx` index is what makes "at most one active run per
   * key" true; without it every concurrent caller inserted its own `active`
   * doc and they all ran at once.
   *
   * Losing the race is not the same as having nothing to do. Under
   * `'pending'` — the single-flight coalescing scope — a caller who finds a
   * run already active queues exactly one follow-up (capped by
   * `dedupe_pending_idx`) and returns `null`, so the work that arrived during
   * the active run is not silently dropped. Under `'pending+active'` a
   * duplicate means "this job already exists"; nothing is queued.
   *
   * @returns a handle when this caller won the slot, `null` otherwise.
   */
  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options: EnqueueOptions = {},
  ): Promise<LeaseAwareJobHandle<T> | null> {
    const now = new Date()
    const dedupeScope: DedupeScope = options.dedupeScope ?? 'pending+active'

    // Short-circuit: a run is already queued, so don't start another now.
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
      if (!this.isDuplicateKeyError(err)) throw err
      // Another caller holds the active slot. Under the coalescing scope,
      // queue at most one follow-up so this request still gets served after
      // the in-flight run finishes; `enqueue` returns null (a no-op) when one
      // is already queued.
      if (options.dedupeKey && dedupeScope === 'pending') {
        await this.enqueue(type, data, options)
      }
      return null
    }
  }

  async claimNext<T>(type: string): Promise<Job<T> | null> {
    const now = new Date()
    // Dedupe identities whose active slot is already taken, discovered as we
    // go. The sort picks the best candidate; if that identity is held, it would
    // win again on every retry, so it must be excluded explicitly.
    const blocked: DedupeIdentity[] = []
    // Failed duplicate-key updates roll back, so one token can fence whichever
    // candidate this call eventually claims.
    const claimUpdate = {
      $set: {
        status: 'active' as JobStatus,
        claimedAt: now,
        claimToken: randomUUID(),
      },
      $inc: { attempt: 1 },
    }

    while (true) {
      const filter: Filter<JobDoc> = {
        type,
        status: 'pending',
        runAt: { $lte: now },
        ...(blocked.length ? { $nor: blocked } : {}),
      }

      try {
        const doc = await this.collection.findOneAndUpdate(
          filter,
          claimUpdate,
          { sort: { priority: 1, runAt: 1 }, returnDocument: 'after' },
        )
        // includeClaimToken: the claim path is the one reader allowed to see the
        // live fencing token (processJob fences lifecycle writes with it).
        return doc ? jobDocToJob(doc as JobDoc<T>, true) : null
      } catch (err) {
        // A dedupe index rejected the pending -> active transition: another job
        // with this dedupeKey is already active. That is the `pending` scope
        // working as designed (at most one active per key), not a failure —
        // there is simply nothing claimable under this key right now.
        //
        // It must not propagate. `JobQueue.claimAndProcess` treats any throw
        // from here as a backend failure and applies exponential backoff to the
        // ProcessorState, which is keyed on job TYPE — so one contended key
        // would stall the poll loop for every other key of that type, with the
        // delay doubling to a minute, while logging errors on a healthy queue.
        const identity = dedupeIdentityFromDuplicateError(err)
        if (identity === null) throw err
        blocked.push(identity)
      }
    }

  }

  /**
   * `$push` fragment for one log entry, bounded to the newest
   * `maxLogEntries` via `$slice` and with the message itself clipped.
   *
   * The bound is what keeps every *terminal* write applicable: `fail`,
   * `failFatal` and the reaper's give-up path all carry a log line in the same
   * update, so an unbounded `logs[]` that reached Mongo's 16MB document cap
   * would fail those writes too — leaving the job stuck `active` with no way
   * to record why. Bounding the array means the terminal write always fits.
   *
   * Log bytes deliberately do **not** feed `journalBytes`: that total guards
   * the step journal (§8.1), and mixing the two let log volume trigger a
   * spurious `JournalTooLarge` on an otherwise small journal.
   */
  private logWrite(
    message: string,
    timestamp = new Date(),
  ): Record<string, unknown> {
    const entry = {
      timestamp,
      message: truncateLogMessage(message, this.maxLogMessageBytes),
    }
    return {
      $push: {
        logs: { $each: [entry], $slice: -this.maxLogEntries },
      },
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
      try {
        const res = await this.collection.updateOne(filter, {
          $set: { status: 'pending' as JobStatus, failReason: reason, runAt },
          ...this.logWrite(`Attempt failed: ${reason}`, now),
        })
        matchedCount = res.matchedCount
      } catch (err) {
        if (dedupeIdentityFromDuplicateError(err) === null) throw err
        // Returning this job to `pending` would put two pending docs under one
        // dedupe key, which `dedupe_pending_idx` forbids. That happens exactly
        // in the case `dedupeScope: 'pending'` exists to allow: a follow-up was
        // queued behind this run while it was active.
        //
        // The queued follow-up already represents the work, so retrying this
        // document would duplicate it. Retire this one instead.
        //
        // It must not be left to propagate: `processJob` catches a failing
        // `fail()`, logs, and moves on — leaving the job `active` forever. The
        // reaper's requeue path performs the same write and would throw the
        // same way, so nothing could ever recover it, and the job would hold
        // its dedupeKey permanently.
        matchedCount = await this.retireCoalesced(
          filter,
          job,
          reason,
          now,
          runAt,
        )
      }
    }
    return claimToken && matchedCount !== 1 ? 'lease-lost' : 'applied'
  }

  /**
   * The pending follower becomes the retry survivor when the active document
   * cannot return to pending. Preserve the consumed attempt and delay so steady
   * traffic cannot bypass retry caps or turn an outage into a retry storm.
   */
  private async carryRetryToFollowUp(
    job: JobDoc,
    runAt: Date,
  ): Promise<void> {
    if (!job.dedupeKey || job.dedupeScope !== 'pending') return
    await this.collection.updateOne(
      {
        dedupeKey: job.dedupeKey,
        dedupeScope: 'pending',
        status: 'pending',
      },
      { $max: { attempt: job.attempt, runAt } },
    )
  }

  /**
   * Mark an active job terminal because its pending follower covers the retry.
   *
   * First replace the lease token under the caller's fence. The active index
   * keeps the follower unclaimable while its retry budget is updated. If the
   * lease changed after the failed requeue, this lock misses and no follower
   * state changes.
   *
   * @returns matchedCount, so callers can apply the usual lease-fence check.
   */
  private async retireCoalesced(
    filter: Filter<JobDoc>,
    job: JobDoc,
    reason: string,
    now: Date,
    runAt: Date,
  ): Promise<number> {
    const retirementToken = randomUUID()
    const lock = await this.collection.updateOne(filter, {
      $set: { claimToken: retirementToken, claimedAt: now },
    })
    if (lock.matchedCount !== 1) return 0

    await this.carryRetryToFollowUp(job, runAt)
    const note = `${reason} (coalesced: a queued follow-up already covers this work)`
    const res = await this.collection.updateOne(
      { _id: job._id, status: 'active', claimToken: retirementToken },
      {
        $set: {
          status: 'failed' as JobStatus,
          failReason: note,
          failedAt: now,
        },
        ...this.logWrite(`Attempt failed, coalesced: ${reason}`, now),
      },
    )
    return res.matchedCount
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

  async heartbeat(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const res = await this.collection.updateOne(
      this.lifecycleFilter(jobId, claimToken),
      {
        $set: { claimedAt: new Date() },
      },
    )
    // matchedCount, not modifiedCount: a same-millisecond claimedAt write is a
    // no-op modification but the lease is still held (see heartbeatClaimed).
    return claimToken && res.matchedCount !== 1 ? 'lease-lost' : 'applied'
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
   * The sweep is **bounded** (`maxPerSweep`, default 1000). After a mass
   * worker death the stuck set can be enormous, and an unbounded sweep walks
   * all of it in one pass — issuing a write per document while the system is
   * already degraded. Draining across several ticks keeps each pass short and
   * predictable; the returned count tells an operator whether it is keeping up
   * (a full batch means there is more waiting).
   *
   * @returns Number of stuck jobs handled (re-queued + failed).
   */
  async recoverStuckJobs(
    visibilityTimeoutMs = 300000,
    maxPerSweep = DEFAULT_REAPER_BATCH_SIZE,
  ): Promise<number> {
    const cutoff = new Date(Date.now() - visibilityTimeoutMs)
    const cursor = this.collection
      .find({
        status: 'active',
        claimedAt: { $lt: cutoff },
      })
      // Oldest lease first: the jobs that have been stuck longest are the ones
      // most worth recovering when the batch cannot cover everything.
      .sort({ claimedAt: 1 })
      .limit(maxPerSweep)

    let handled = 0
    for await (const job of cursor) {
      const now = new Date()
      const exhausted = job.attempt >= job.maxAttempts
      const leaseFilter: Filter<JobDoc> = {
        _id: job._id,
        status: 'active',
        claimedAt: job.claimedAt,
        ...(job.claimToken ? { claimToken: job.claimToken } : {}),
      }
      let matchedCount = 0

      if (exhausted) {
        const result = await this.collection.updateOne(
          leaseFilter,
          {
            $set: {
              status: 'failed' as JobStatus,
              failReason: 'Stalled — retries exhausted',
              failedAt: now,
            },
            ...this.logWrite('Stalled — retries exhausted', now),
          },
        )
        matchedCount = result.matchedCount
      } else {
        const runAt = new Date(now.getTime() + retryBackoffMs(job.attempt, job))
        try {
          const result = await this.collection.updateOne(
            leaseFilter,
            {
              $set: { status: 'pending' as JobStatus, runAt },
              ...this.logWrite('Recovered (stalled)', now),
            },
          )
          matchedCount = result.matchedCount
        } catch (err) {
          if (dedupeIdentityFromDuplicateError(err) === null) throw err
          // Same collision as in `fail()`: a follow-up is already queued under
          // this dedupe key, so returning this one to `pending` is forbidden and
          // unnecessary. Retire it so the active slot frees.
          //
          // Without this the reaper threw on every sweep for this job, so a
          // stalled job whose key had a queued follow-up could never be
          // recovered by anything — it held the key forever.
          matchedCount = await this.retireCoalesced(
            leaseFilter,
            job,
            'Stalled',
            now,
            runAt,
          )
        }
      }
      handled += matchedCount
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
  ): LeaseAwareJobHandle<T> {
    return {
      id: jobId,
      data,
      complete: async () => {
        await this.complete(jobId, claimToken)
      },
      fail: async (reason: string) => {
        await this.fail(jobId, reason, claimToken)
      },
      heartbeat: () => this.heartbeat(jobId, claimToken),
      log: (msg: string) => {
        // See JobQueue.createContext: an uncaught rejection here would kill
        // the process over a dropped log line.
        this.log(jobId, msg).catch((err: unknown) => {
          this.logger.warn({ err, jobId }, 'failed to write job log entry')
        })
      },
    }
  }

  private isDuplicateKeyError(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false
    const e = err as { code?: number; message?: string }
    return e.code === 11000 || (e.message?.includes('E11000') ?? false)
  }
}
