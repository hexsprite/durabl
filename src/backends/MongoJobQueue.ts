/** MongoJobQueue - MongoDB-backed durable job queue. */
import { randomUUID } from 'node:crypto'

import type { Collection, Db } from 'mongodb'

import { defaultLogger, type Logger } from '../logger'
import {
  type DedupeScope,
  type EnqueueOptions,
  type Job,
  type JobDoc,
  jobDocToJob,
  type JobHandle,
  type JobStatus,
  type QueueStats,
} from '../types'

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
}

type JobAvailableListener = (type: string) => void

export class MongoJobQueue implements IJobQueueBackend {
  private db: Db
  private collection: Collection<JobDoc>
  private useChangeStreams: boolean
  private logger: Logger
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
      runAt,
      createdAt: now,
      logs: [],
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
      runAt: now,
      createdAt: now,
      claimedAt: now,
      logs: [],
    }

    try {
      await this.collection.insertOne(doc as JobDoc)
      return this.createHandle(doc._id, data)
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
        $set: { status: 'active' as JobStatus, claimedAt: now },
        $inc: { attempt: 1 },
      },
      { sort: { priority: 1, runAt: 1 }, returnDocument: 'after' },
    )
    return doc ? jobDocToJob(doc as JobDoc<T>) : null
  }

  async complete(jobId: string): Promise<void> {
    await this.collection.updateOne(
      { _id: jobId },
      {
        $set: { status: 'completed' as JobStatus, completedAt: new Date() },
      },
    )
  }

  async fail(jobId: string, reason: string): Promise<void> {
    const job = await this.collection.findOne({ _id: jobId })
    if (!job) return

    const now = new Date()
    const exhausted = job.attempt >= job.maxAttempts

    if (exhausted) {
      await this.collection.updateOne(
        { _id: jobId },
        {
          $set: {
            status: 'failed' as JobStatus,
            failReason: reason,
            failedAt: now,
          },
          $push: { logs: { timestamp: now, message: `Failed: ${reason}` } },
        },
      )
    } else {
      await this.collection.updateOne(
        { _id: jobId },
        {
          $set: { status: 'pending' as JobStatus, failReason: reason },
          $push: {
            logs: { timestamp: now, message: `Attempt failed: ${reason}` },
          },
        },
      )
    }
  }

  async failFatal(jobId: string, reason: string): Promise<void> {
    const now = new Date()
    await this.collection.updateOne(
      { _id: jobId },
      {
        $set: {
          status: 'failed' as JobStatus,
          failReason: reason,
          failedAt: now,
        },
        $push: { logs: { timestamp: now, message: `Fatal: ${reason}` } },
      },
    )
  }

  async log(jobId: string, message: string): Promise<void> {
    await this.collection.updateOne(
      { _id: jobId },
      {
        $push: { logs: { timestamp: new Date(), message } },
      },
    )
  }

  async heartbeat(jobId: string): Promise<void> {
    await this.collection.updateOne(
      { _id: jobId },
      {
        $set: { claimedAt: new Date() },
      },
    )
  }

  async findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null> {
    const doc = (await this.collection.findOne(query)) as JobDoc<T> | null
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

  /** Recover stuck jobs past visibility timeout. Returns count reset. */
  async recoverStuckJobs(visibilityTimeoutMs = 300000): Promise<number> {
    const cutoff = new Date(Date.now() - visibilityTimeoutMs)
    const result = await this.collection.updateMany(
      { status: 'active', claimedAt: { $lt: cutoff } },
      {
        $set: { status: 'pending' as JobStatus },
        $push: { logs: { timestamp: new Date(), message: 'Recovered' } },
      },
    )
    return result.modifiedCount
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

  private createHandle<T>(jobId: string, data: T): JobHandle<T> {
    return {
      id: jobId,
      data,
      complete: () => this.complete(jobId),
      fail: (reason: string) => this.fail(jobId, reason),
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
