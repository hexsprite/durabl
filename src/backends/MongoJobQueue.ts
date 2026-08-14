/** MongoJobQueue - MongoDB-backed durable job queue. */
import { randomUUID } from 'node:crypto'

import type { Collection, Db, Filter } from 'mongodb'

import { defaultLogger, type Logger } from '../logger'
import { JournalTooLarge } from '../journal/errors'
import {
  assertStepMatches,
  DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
  DEFAULT_MAX_LOG_ENTRIES,
  DEFAULT_MAX_LOG_MESSAGE_BYTES,
  guardAppend,
  sortBySeq,
  truncateLogMessage,
} from '../journal/serialize'
import {
  assertValidLatestCoalescing,
  type AppendStepResult,
  type CompleteClaimedResult,
  type CompleteJobResult,
  type DedupeScope,
  type EnqueueOptions,
  type FailFatalJobResult,
  type FailJobResult,
  type HeartbeatClaimedResult,
  type Job,
  type JobDoc,
  jobDocToJob,
  type JobHandle,
  type JobStatus,
  type LifecycleWriteResult,
  type ReleaseJobResult,
  type QueueStats,
  type TerminalReceipt,
  type TerminalWriteMissResult,
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

/**
 * How many dedupe-blocked candidates one {@link MongoJobQueue.claimNext} call
 * will step over before yielding to the poll loop.
 *
 * Each skip costs a round trip, so an unbounded loop turns one claim into an
 * unbounded number of them precisely when the queue is most contended. Yielding
 * is cheap and safe: the poll loop returns shortly, and a key that frees up in
 * the meantime is claimable then.
 */
const MAX_DEDUPE_SKIPS = 20
/** Maximum upsert retries after a concurrent pending-follower insert wins. */
const MAX_LATEST_COALESCE_ATTEMPTS = 3

/** Fields returned by hot claim reads; journals and receipts can be megabytes. */
const CLAIMED_JOB_PROJECTION = {
  _id: 1,
  type: 1,
  data: 1,
  status: 1,
  attempt: 1,
  maxAttempts: 1,
  priority: 1,
  dedupeKey: 1,
  dedupeScope: 1,
  runAt: 1,
  createdAt: 1,
  claimedAt: 1,
  completedAt: 1,
  failedAt: 1,
  failReason: 1,
  claimToken: 1,
} as const

/** Fields needed to choose a retry transition and its delay. */
const RETRY_JOB_PROJECTION = {
  _id: 1,
  attempt: 1,
  maxAttempts: 1,
  dedupeKey: 1,
  dedupeScope: 1,
  backoff: 1,
  backoffDelay: 1,
  backoffMaxDelay: 1,
} as const

/**
 * The `dedupeKey` a duplicate-key error was raised on, or `null` if this is not
 * a dedupe-index violation and therefore not ours to swallow.
 *
 * Reads the driver's structured `keyValue` rather than parsing the message —
 * message text is not a stable interface, and a partial parse would silently
 * turn a real error into a skipped candidate. A duplicate key on any other
 * index is a genuine fault and must propagate.
 */
function dedupeKeyFromDuplicateError(err: unknown): string | null {
  if (!err || typeof err !== 'object') return null
  const e = err as {
    code?: unknown
    keyValue?: Record<string, unknown>
    keyPattern?: Record<string, unknown>
  }
  if (e.code !== 11000) return null
  const key = e.keyValue?.dedupeKey ?? null
  if (typeof key !== 'string') return null
  // Guard against a same-named field on some unrelated future index.
  if (e.keyPattern && !('dedupeKey' in e.keyPattern)) return null
  return key
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

export class MongoJobQueue implements IJobQueueBackend {
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
   * Mutual exclusion comes from the unique partial indexes. In latest mode, an
   * atomic pending update replaces only `data`; an upsert creates the follower
   * if no pending job remains. The active document is never updated.
   *
   * A duplicate active insert means another caller holds the slot. Pending
   * scope queues one follower, capped by `dedupe_pending_idx`. Latest mode
   * updates that follower's payload; legacy mode keeps its first payload.
   * Pending+active scope creates no follower.
   *
   * @returns a handle when this caller won the slot, `null` otherwise.
   */
  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options: EnqueueOptions = {},
  ): Promise<JobHandle<T> | null> {
    assertValidLatestCoalescing(options)
    const now = new Date()
    const dedupeScope: DedupeScope = options.dedupeScope ?? 'pending+active'

    if (options.coalesce === 'latest') {
      const replaced = await this.collection.updateOne(
        {
          type,
          dedupeKey: options.dedupeKey,
          dedupeScope: 'pending',
          status: 'pending',
        },
        { $set: { data } },
      )
      if (replaced.matchedCount === 1) return null
    } else if (options.dedupeKey) {
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
      return this.createHandle(jobDocToJob(doc, true))
    } catch (err) {
      const duplicateKey = dedupeKeyFromDuplicateError(err)
      if (!options.dedupeKey || duplicateKey !== options.dedupeKey) throw err
      if (dedupeScope === 'pending') {
        if (options.coalesce === 'latest') {
          await this.upsertLatestFollower(
            type,
            data,
            options.dedupeKey,
            options,
          )
        } else {
          await this.enqueue(type, data, options)
        }
      }
      return null
    }
  }

  private async upsertLatestFollower<T>(
    type: string,
    data: T,
    dedupeKey: string,
    options: EnqueueOptions,
  ): Promise<void> {
    const createdAt = new Date()
    const runAt = options.delay
      ? new Date(createdAt.getTime() + options.delay)
      : createdAt
    const followerId = randomUUID()

    for (
      let attempt = 1;
      attempt <= MAX_LATEST_COALESCE_ATTEMPTS;
      attempt++
    ) {
      try {
        await this.collection.updateOne(
          { type, dedupeKey, dedupeScope: 'pending', status: 'pending' },
          {
            $set: { data },
            $setOnInsert: {
              _id: followerId,
              type,
              status: 'pending' as JobStatus,
              priority: options.priority ?? 0,
              attempt: 0,
              maxAttempts: options.maxAttempts ?? 3,
              dedupeKey,
              dedupeScope: 'pending' as DedupeScope,
              backoff: options.backoff,
              backoffDelay: options.backoffDelay,
              backoffMaxDelay: options.backoffMaxDelay,
              runAt,
              createdAt,
              logs: [],
              journalBytes: 0,
            },
          },
          { upsert: true },
        )
        return
      } catch (err) {
        if (dedupeKeyFromDuplicateError(err) !== dedupeKey) throw err
        const holder = await this.collection.findOne(
          { dedupeKey, dedupeScope: 'pending', status: 'pending' },
          { projection: { type: 1 } },
        )
        if (holder && holder.type !== type) return
        if (attempt === MAX_LATEST_COALESCE_ATTEMPTS) throw err
      }
    }
  }

  async claimNext<T>(type: string): Promise<Job<T> | null> {
    const now = new Date()
    // dedupeKeys whose active slot is already taken, discovered as we go. The
    // sort picks the best candidate; if that candidate's key is held, it would
    // win the sort again on every retry, so it has to be excluded explicitly or
    // the loop spins on it.
    const blockedKeys: string[] = []

    for (let attempt = 0; attempt <= MAX_DEDUPE_SKIPS; attempt++) {
      const filter: Filter<JobDoc> = {
        type,
        status: 'pending',
        runAt: { $lte: now },
        ...(blockedKeys.length ? { dedupeKey: { $nin: blockedKeys } } : {}),
      }

      try {
        const doc = await this.collection.findOneAndUpdate(
          filter,
          {
            // Mint a fresh per-claim lease nonce. All orchestration fencing keys
            // on this, not on `attempt` (R8) — an admin/manual re-activation
            // that does not bump `attempt` would otherwise silently break the
            // fence.
            $set: {
              status: 'active' as JobStatus,
              claimedAt: now,
              claimToken: randomUUID(),
            },
            $inc: { attempt: 1 },
          },
          {
            sort: { priority: 1, runAt: 1 },
            returnDocument: 'after',
            projection: CLAIMED_JOB_PROJECTION,
          },
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
        const key = dedupeKeyFromDuplicateError(err)
        if (key === null) throw err
        blockedKeys.push(key)
      }
    }

    // Exhausted the skip budget. Unbounded retrying would turn one claim into
    // an unbounded number of round trips on a deeply contended backlog, so we
    // yield instead: the poll loop comes back, and any key that frees up in the
    // meantime is claimable then.
    this.logger.debug(
      { type, blocked: blockedKeys.length },
      'claimNext yielded: dedupe-blocked candidates exceeded skip budget',
    )
    return null
  }

  async claimNextByKey<T>(
    type: string,
    dedupeKey: string,
  ): Promise<JobHandle<T> | null> {
    const now = new Date()
    try {
      const doc = await this.collection.findOneAndUpdate(
        {
          type,
          dedupeKey,
          status: 'pending',
          runAt: { $lte: now },
        },
        {
          $set: {
            status: 'active' as JobStatus,
            claimedAt: now,
            claimToken: randomUUID(),
          },
          $inc: { attempt: 1 },
        },
        {
          sort: { priority: 1, runAt: 1 },
          returnDocument: 'after',
          projection: CLAIMED_JOB_PROJECTION,
        },
      )
      return doc
        ? this.createHandle(jobDocToJob(doc as JobDoc<T>, true))
        : null
    } catch (err) {
      if (this.isDuplicateKeyError(err)) return null
      throw err
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
    return claimToken !== undefined
      ? { _id: jobId, status: 'active' as JobStatus, claimToken }
      : { _id: jobId }
  }

  /** Terminal writes never overwrite an existing terminal state. */
  private terminalTransitionFilter(
    jobId: string,
    claimToken?: string,
  ): Filter<JobDoc> {
    return claimToken !== undefined
      ? { _id: jobId, status: 'active', claimToken }
      : { _id: jobId, status: { $in: ['pending', 'active'] } }
  }

  private async classifyTerminalMiss(
    jobId: string,
  ): Promise<TerminalWriteMissResult> {
    const job = await this.collection.findOne(
      { _id: jobId },
      { projection: { status: 1 } },
    )
    if (!job) return { status: 'not-found' }
    if (
      job.status === 'completed' ||
      job.status === 'failed' ||
      job.status === 'superseded'
    ) {
      return { status: 'already-terminal', terminalStatus: job.status }
    }
    return { status: 'lease-lost' }
  }

  private terminalReceiptKey(claimToken: string | undefined): string {
    return claimToken === undefined
      ? 'unfenced'
      : `token_${Buffer.from(claimToken).toString('base64url')}`
  }

  private terminalReceiptPath(
    claimToken: string | undefined,
    operation: TerminalReceipt['operation'],
  ): string {
    return `terminalReceipts.${this.terminalReceiptKey(claimToken)}.${operation}`
  }

  private terminalReceiptWrite(
    receipt: TerminalReceipt,
  ): Record<string, TerminalReceipt['result']> {
    return {
      [this.terminalReceiptPath(
        receipt.claimToken ?? undefined,
        receipt.operation,
      )]: receipt.result,
    }
  }

  private async reconcileTerminalReceipt<
    R extends
      | CompleteJobResult
      | FailJobResult
      | FailFatalJobResult
      | ReleaseJobResult,
  >(
    jobId: string,
    claimToken: string | undefined,
    operation: TerminalReceipt['operation'],
  ): Promise<R | null> {
    const key = this.terminalReceiptKey(claimToken)
    const path = this.terminalReceiptPath(claimToken, operation)
    const job = await this.collection.findOne(
      { _id: jobId },
      { projection: { [path]: 1 } },
    )
    const result = job?.terminalReceipts?.[key]?.[operation]
    return (result as R | undefined) ?? null
  }

  async complete(
    jobId: string,
    claimToken?: string,
  ): Promise<CompleteJobResult> {
    const filter =
      claimToken === undefined
        ? this.terminalTransitionFilter(jobId, claimToken)
        : {
            _id: jobId,
            claimToken,
            $or: [
              { status: 'active' as JobStatus },
              {
                status: 'failed' as JobStatus,
                [`terminalReceipts.${this.terminalReceiptKey(claimToken)}`]: {
                  $exists: false,
                },
              },
            ],
          }
    const result = { status: 'completed' } as const
    let matchedCount: number
    try {
      const write = await this.collection.updateOne(
        filter,
        {
          $set: {
            status: 'completed' as JobStatus,
            completedAt: new Date(),
            ...this.terminalReceiptWrite({
              claimToken: claimToken ?? null,
              operation: 'complete',
              result,
            }),
          },
          $unset: { failedAt: '', failReason: '' },
        },
      )
      matchedCount = write.matchedCount
    } catch (err) {
      const reconciled =
        await this.reconcileTerminalReceipt<CompleteJobResult>(
          jobId,
          claimToken,
          'complete',
        )
      if (reconciled) return reconciled
      throw err
    }
    return matchedCount === 1 ? result : this.classifyTerminalMiss(jobId)
  }

  async fail(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<FailJobResult> {
    const filter = this.terminalTransitionFilter(jobId, claimToken)
    const job = await this.collection.findOne(filter, {
      projection: RETRY_JOB_PROJECTION,
    })
    if (!job) return this.classifyTerminalMiss(jobId)

    const now = new Date()
    if (job.dedupeKey && job.dedupeScope === 'pending') {
      const follower = await this.collection.findOne(
        {
          dedupeKey: job.dedupeKey,
          dedupeScope: 'pending',
          status: 'pending',
        },
        { projection: { _id: 1 } },
      )
      if (follower) {
        return this.supersedeFailedClaim(
          filter,
          jobId,
          reason,
          now,
          claimToken,
        )
      }
    }
    if (job.attempt >= job.maxAttempts) {
      const result = { status: 'failed-terminal' } as const
      let matchedCount: number
      try {
        const write = await this.collection.updateOne(filter, {
          $set: {
            status: 'failed' as JobStatus,
            failReason: reason,
            failedAt: now,
            ...this.terminalReceiptWrite({
              claimToken: claimToken ?? null,
              operation: 'fail',
              result,
            }),
          },
          ...this.logWrite(`Failed: ${reason}`, now),
        })
        matchedCount = write.matchedCount
      } catch (err) {
        const reconciled = await this.reconcileTerminalReceipt<FailJobResult>(
          jobId,
          claimToken,
          'fail',
        )
        if (reconciled) return reconciled
        throw err
      }
      return matchedCount === 1 ? result : this.classifyTerminalMiss(jobId)
    }

    // Space the retry: push runAt into the future by a jittered backoff so
    // a fast-failing handler cannot burn every attempt immediately.
    const runAt = new Date(now.getTime() + retryBackoffMs(job.attempt, job))
    const retryResult = { status: 'retry-scheduled' } as const
    let matchedCount: number
    try {
      const write = await this.collection.updateOne(filter, {
        $set: {
          status: 'pending' as JobStatus,
          failReason: reason,
          runAt,
          ...this.terminalReceiptWrite({
            claimToken: claimToken ?? null,
            operation: 'fail',
            result: retryResult,
          }),
        },
        ...this.logWrite(`Attempt failed: ${reason}`, now),
      })
      matchedCount = write.matchedCount
    } catch (err) {
      const reconciled = await this.reconcileTerminalReceipt<FailJobResult>(
        jobId,
        claimToken,
        'fail',
      )
      if (reconciled) return reconciled
      if (dedupeKeyFromDuplicateError(err) === null) throw err

      return this.supersedeFailedClaim(
        filter,
        jobId,
        reason,
        now,
        claimToken,
      )
    }
    return matchedCount === 1
      ? retryResult
      : this.classifyTerminalMiss(jobId)
  }

  /**
   * Mark a job superseded because a pending job under the same dedupe key
   * already covers its work. Used when a requeue would violate a dedupe index.
   *
   * @returns matchedCount, so callers can apply the usual lease-fence check.
   */
  private async retireCoalesced(
    filter: Filter<JobDoc>,
    reason: string,
    now: Date,
    terminalReceipt?: TerminalReceipt,
  ): Promise<number> {
    const note = `${reason} (superseded: a queued follow-up already covers this work)`
    const res = await this.collection.updateOne(filter, {
      $set: {
        status: 'superseded' as JobStatus,
        failReason: note,
        failedAt: now,
        ...(terminalReceipt
          ? this.terminalReceiptWrite(terminalReceipt)
          : {}),
      },
      ...this.logWrite(`Superseded: ${reason}`, now),
    })
    return res.matchedCount
  }

  private async supersedeFailedClaim(
    filter: Filter<JobDoc>,
    jobId: string,
    reason: string,
    now: Date,
    claimToken?: string,
  ): Promise<FailJobResult> {
    const result = { status: 'superseded' } as const
    let matchedCount: number
    try {
      matchedCount = await this.retireCoalesced(filter, reason, now, {
        claimToken: claimToken ?? null,
        operation: 'fail',
        result,
      })
    } catch (err) {
      const reconciled = await this.reconcileTerminalReceipt<FailJobResult>(
        jobId,
        claimToken,
        'fail',
      )
      if (reconciled) return reconciled
      throw err
    }
    return matchedCount === 1 ? result : this.classifyTerminalMiss(jobId)
  }

  async failFatal(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<FailFatalJobResult> {
    const result = { status: 'failed-terminal' } as const
    const now = new Date()
    let matchedCount: number
    try {
      const write = await this.collection.updateOne(
        this.terminalTransitionFilter(jobId, claimToken),
        {
          $set: {
            status: 'failed' as JobStatus,
            failReason: reason,
            failedAt: now,
            ...this.terminalReceiptWrite({
              claimToken: claimToken ?? null,
              operation: 'failFatal',
              result,
            }),
          },
          ...this.logWrite(`Fatal: ${reason}`, now),
        },
      )
      matchedCount = write.matchedCount
    } catch (err) {
      const reconciled =
        await this.reconcileTerminalReceipt<FailFatalJobResult>(
          jobId,
          claimToken,
          'failFatal',
        )
      if (reconciled) return reconciled
      throw err
    }
    return matchedCount === 1 ? result : this.classifyTerminalMiss(jobId)
  }

  async release(
    jobId: string,
    claimToken?: string,
  ): Promise<ReleaseJobResult> {
    const filter: Filter<JobDoc> =
      claimToken === undefined
        ? { _id: jobId, status: 'active' }
        : { _id: jobId, status: 'active', claimToken }
    const released = { status: 'released' } as const
    let matchedCount: number
    try {
      const write = await this.collection.updateOne(filter, {
        $set: {
          status: 'pending' as JobStatus,
          runAt: new Date(),
          ...this.terminalReceiptWrite({
            claimToken: claimToken ?? null,
            operation: 'release',
            result: released,
          }),
        },
        $unset: { claimedAt: '', claimToken: '' },
      })
      matchedCount = write.matchedCount
    } catch (err) {
      const reconciled =
        await this.reconcileTerminalReceipt<ReleaseJobResult>(
          jobId,
          claimToken,
          'release',
        )
      if (reconciled) return reconciled
      if (dedupeKeyFromDuplicateError(err) === null) throw err

      const superseded = { status: 'superseded' } as const
      let retired: number
      try {
        retired = await this.retireCoalesced(
          filter,
          'Released',
          new Date(),
          {
            claimToken: claimToken ?? null,
            operation: 'release',
            result: superseded,
          },
        )
      } catch (retireErr) {
        const retiredReceipt =
          await this.reconcileTerminalReceipt<ReleaseJobResult>(
            jobId,
            claimToken,
            'release',
          )
        if (retiredReceipt) return retiredReceipt
        throw retireErr
      }
      return retired === 1
        ? superseded
        : this.classifyTerminalMiss(jobId)
    }
    if (matchedCount === 1) return released

    return this.classifyTerminalMiss(jobId)
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
    assertStepMatches(jobId, record.seq, existing.name, record.name)
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

  async hasOutstanding(type: string, dedupeKey: string): Promise<boolean> {
    const doc = await this.collection.findOne(
      {
        type,
        dedupeKey,
        status: { $in: ['pending', 'active'] satisfies JobStatus[] },
      },
      { projection: { _id: 1 } },
    )
    return doc !== null
  }

  async findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null> {
    const doc = (await this.collection.findOne(query)) as JobDoc<T> | null
    // General read view: the live fencing claimToken is deliberately NOT
    // exposed here — only the claim path (claimNext) may see it.
    return doc ? jobDocToJob(doc) : null
  }

  async getStats(type?: string): Promise<QueueStats> {
    const now = new Date()
    const q = type ? { type } : {}
    const count = (s: JobStatus) =>
      this.collection.countDocuments({ ...q, status: s })
    const [pending, active, completed, failed, superseded, oldest] =
      await Promise.all([
        count('pending'),
        count('active'),
        count('completed'),
        count('failed'),
        count('superseded'),
        // Backlog age. `runAt: {$lte: now}` keeps deliberately-delayed jobs out
        // of the measurement — they are scheduled, not late. Sorting on `runAt`
        // alone (not the claim order) is what makes this "oldest", and the
        // `{type, status, priority, runAt}` claim index serves the predicate;
        // no additional index is needed.
        this.collection
          .find({ ...q, status: 'pending', runAt: { $lte: now } })
          .project<{ runAt: Date }>({ runAt: 1 })
          .sort({ runAt: 1 })
          .limit(1)
          .next(),
      ])
    const oldestPendingRunAt = oldest?.runAt ?? null
    return {
      pending,
      active,
      completed,
      failed,
      superseded,
      oldestPendingRunAt,
      oldestPendingLagMs: oldestPendingRunAt
        ? Math.max(0, now.getTime() - oldestPendingRunAt.getTime())
        : 0,
    }
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
   * - A pending follower already covers the work → terminal `superseded`,
   *   regardless of the old run's remaining attempts.
   * - Retries exhausted (`attempt >= maxAttempts`) without a follower →
   *   terminal `failed`. The old code resurrected these forever because it
   *   never checked the cap.
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
      .project(RETRY_JOB_PROJECTION)
      // Oldest lease first: the jobs that have been stuck longest are the ones
      // most worth recovering when the batch cannot cover everything.
      .sort({ claimedAt: 1 })
      .limit(maxPerSweep)

    let handled = 0
    for await (const job of cursor) {
      const now = new Date()
      const exhausted = job.attempt >= job.maxAttempts
      const hasPendingFollower =
        exhausted &&
        job.dedupeKey !== undefined &&
        job.dedupeScope === 'pending' &&
        (await this.collection.findOne(
          {
            dedupeKey: job.dedupeKey,
            dedupeScope: 'pending',
            status: 'pending',
          },
          { projection: { _id: 1 } },
        )) !== null

      if (hasPendingFollower) {
        await this.retireCoalesced(
          { _id: job._id, status: 'active' },
          'Stalled',
          now,
        )
        handled++
        continue
      }

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
        try {
          await this.collection.updateOne(
            { _id: job._id, status: 'active' },
            {
              $set: { status: 'pending' as JobStatus, runAt },
              ...this.logWrite('Recovered (stalled)', now),
            },
          )
        } catch (err) {
          if (dedupeKeyFromDuplicateError(err) === null) throw err
          // Same collision as in `fail()`: a follow-up is already queued under
          // this dedupe key, so returning this one to `pending` is forbidden and
          // unnecessary. Retire it so the active slot frees.
          //
          // Without this the reaper threw on every sweep for this job, so a
          // stalled job whose key had a queued follow-up could never be
          // recovered by anything — it held the key forever.
          await this.retireCoalesced(
            { _id: job._id, status: 'active' },
            'Stalled',
            now,
          )
        }
      }
      handled++
    }
    return handled
  }

  /** Clean up old terminal jobs. Default: 7 days. Returns count removed. */
  async cleanupOldJobs(maxAgeMs = 7 * 24 * 60 * 60 * 1000): Promise<number> {
    const cutoff = new Date(Date.now() - maxAgeMs)
    const result = await this.collection.deleteMany({
      $or: [
        { status: 'completed', completedAt: { $lt: cutoff } },
        {
          status: { $in: ['failed', 'superseded'] },
          failedAt: { $lt: cutoff },
        },
      ],
    })
    return result.deletedCount
  }

  getCollection(): Collection<JobDoc> {
    return this.collection
  }

  async resetStorage(): Promise<void> {
    await this.collection.deleteMany({})
  }

  private createHandle<T>(job: Job<T>): JobHandle<T> {
    const claimToken = job.claimToken
    return {
      ...job,
      status: 'active',
      complete: () => this.complete(job.id, claimToken),
      fail: (reason: string) => this.fail(job.id, reason, claimToken),
      failFatal: (reason: string) =>
        this.failFatal(job.id, reason, claimToken),
      heartbeat: () => this.heartbeat(job.id, claimToken),
      release: () => this.release(job.id, claimToken),
      log: (message: string) => {
        this.log(job.id, message).catch((err: unknown) => {
          this.logger.warn({ err, jobId: job.id }, 'failed to write job log entry')
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
