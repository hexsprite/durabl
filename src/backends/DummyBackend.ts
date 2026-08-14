/**
 * DummyBackend - For Unit Tests
 *
 * Records all operations without executing handlers.
 * Use to verify job creation, dedupe behavior, and arguments.
 */

import { randomUUID } from 'node:crypto'

import {
  appendStepInMemory,
  completeClaimedInMemory,
  heartbeatClaimedInMemory,
} from '../journal/inMemory'
import {
  DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
  DEFAULT_MAX_LOG_ENTRIES,
  DEFAULT_MAX_LOG_MESSAGE_BYTES,
  sortBySeq,
  truncateLogMessage,
} from '../journal/serialize'
import { assertValidLatestCoalescing } from '../types'
import type {
  AppendStepResult,
  CompleteClaimedResult,
  CompleteJobResult,
  EnqueueOptions,
  FailFatalJobResult,
  FailJobResult,
  HeartbeatClaimedResult,
  Job,
  JobHandle,
  JobStatus,
  LifecycleWriteResult,
  ReleaseJobResult,
  TerminalWriteMissResult,
  QueueStats,
  StepRecord,
} from '../types'

import { backlogAge } from './backlogAge'
import type { IJobQueueBackend } from './IJobQueueBackend'

interface RecordedJob<T = unknown> {
  id: string
  type: string
  data: T
  status: JobStatus
  priority: number
  attempt: number
  maxAttempts: number
  dedupeKey?: string
  dedupeScope?: 'pending' | 'pending+active'
  createdAt: Date
  /** When the job becomes due (`createdAt + delay`). */
  runAt: Date
  logs: string[]
  claimToken?: string
  claimedAt?: Date
  failedAt?: Date
  failReason?: string
  /** Distinguishes a lifecycle failure from a reaper race in `complete()`. */
  failedByLifecycleWrite?: boolean
  steps: StepRecord[]
  /** Running approximate byte size of steps + logs (mirrors Mongo's field). */
  journalBytes: number
}

/**
 * DummyBackend records job operations for test assertions.
 *
 * @example
 * ```typescript
 * const backend = new DummyBackend()
 * JobQueue.setBackend(backend)
 *
 * await myService.doSomething()
 *
 * expect(backend.jobs).toHaveLength(1)
 * expect(backend.jobs[0].type).toBe('myJobType')
 * ```
 */
export class DummyBackend implements IJobQueueBackend {
  /** All recorded jobs */
  jobs: RecordedJob[] = []

  /** Retained `logs` entries per job; mirrors MongoJobQueue's `$slice` bound. */
  maxLogEntries = DEFAULT_MAX_LOG_ENTRIES
  /** Per-message ceiling; mirrors MongoJobQueue's message clipping. */
  maxLogMessageBytes = DEFAULT_MAX_LOG_MESSAGE_BYTES

  /** `visibilityTimeoutMs` value of every `recoverStuckJobs()` call, in order. */
  recoverStuckJobsCalls: (number | undefined)[] = []

  /** Counter for generating IDs */
  private idCounter = 0

  /**
   * Generate a unique job ID
   */
  private generateId(): string {
    this.idCounter++
    return `dummy-${this.idCounter}`
  }

  /** Build the public view shared by every claim path. */
  private toJob<T>(job: RecordedJob<T>, includeClaimToken = false): Job<T> {
    return {
      id: job.id,
      type: job.type,
      data: job.data,
      status: job.status,
      attempt: job.attempt,
      maxAttempts: job.maxAttempts,
      priority: job.priority,
      dedupeKey: job.dedupeKey,
      dedupeScope: job.dedupeScope,
      runAt: job.runAt,
      createdAt: job.createdAt,
      claimedAt: job.claimedAt,
      ...(includeClaimToken ? { claimToken: job.claimToken } : {}),
    }
  }

  /** Bind a claim's lifecycle methods to its immutable lease token. */
  private createHandle<T>(job: RecordedJob<T>): JobHandle<T> {
    const claimToken = job.claimToken
    return {
      ...this.toJob(job, true),
      status: 'active',
      complete: () => this.complete(job.id, claimToken),
      fail: (reason: string) => this.fail(job.id, reason, claimToken),
      failFatal: (reason: string) =>
        this.failFatal(job.id, reason, claimToken),
      heartbeat: () => this.heartbeat(job.id, claimToken),
      release: () => this.release(job.id, claimToken),
      log: (message: string) => {
        void this.log(job.id, message)
      },
    }
  }

  private markClaimed(job: RecordedJob): void {
    job.status = 'active'
    job.attempt++
    job.claimedAt = new Date()
    job.claimToken = randomUUID()
  }

  /**
   * Find existing job by dedupeKey respecting dedupeScope
   */
  private findByDedupeKey(
    dedupeKey: string,
    dedupeScope: 'pending' | 'pending+active',
  ): RecordedJob | undefined {
    return this.jobs.find((job) => {
      if (job.dedupeKey !== dedupeKey) return false
      if (job.dedupeScope !== dedupeScope) return false

      if (dedupeScope === 'pending') {
        return job.status === 'pending'
      } else {
        return job.status === 'pending' || job.status === 'active'
      }
    })
  }

  async enqueue(
    type: string,
    data: unknown,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    const dedupeScope = options.dedupeScope ?? 'pending+active'

    // Check for duplicate
    if (options.dedupeKey) {
      const existing = this.findByDedupeKey(options.dedupeKey, dedupeScope)
      if (existing) {
        return null
      }
    }

    const createdAt = new Date()
    const job: RecordedJob = {
      id: this.generateId(),
      type,
      data,
      status: 'pending',
      priority: options.priority ?? 0,
      attempt: 0,
      maxAttempts: options.maxAttempts ?? 3,
      dedupeKey: options.dedupeKey,
      dedupeScope: options.dedupeKey ? dedupeScope : undefined,
      createdAt,
      runAt: new Date(createdAt.getTime() + (options.delay ?? 0)),
      logs: [],
      steps: [],
      journalBytes: 0,
    }

    this.jobs.push(job)
    return job.id
  }

  /**
   * Mirrors {@link MongoJobQueue.claimOrEnqueue}, including the part Mongo
   * enforces with a unique partial index rather than application logic: at most
   * one *active* run per dedupe key. Modelling it here is what lets a unit test
   * catch a broken single-flight assumption without booting Mongo — if this
   * backend is more permissive than the real one, the tests certify a lie.
   */
  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options: EnqueueOptions = {},
  ): Promise<JobHandle<T> | null> {
    assertValidLatestCoalescing(options)
    const dedupeScope = options.dedupeScope ?? 'pending+active'

    if (options.dedupeKey) {
      // A run is already queued — don't start another now.
      const pending = this.jobs.find(
        (job) =>
          job.dedupeKey === options.dedupeKey && job.status === 'pending',
      )
      if (pending) {
        if (options.coalesce === 'latest' && pending.type === type) {
          pending.data = data
        }
        return null
      }

      // Stand-in for the unique partial indexes: a job already holds the
      // active slot for this key+scope.
      const active = this.jobs.find(
        (job) =>
          job.dedupeKey === options.dedupeKey &&
          job.dedupeScope === dedupeScope &&
          job.status === 'active',
      )
      if (active) {
        // Coalescing scope: queue exactly one follow-up (enqueue itself caps
        // that at one) so the request is served after the active run ends.
        if (dedupeScope === 'pending') {
          await this.enqueue(type, data, options)
        }
        return null
      }
    }

    // Create and immediately claim
    const claimedAt = new Date()
    const job: RecordedJob<T> = {
      id: this.generateId(),
      type,
      data,
      status: 'active',
      priority: options.priority ?? 0,
      attempt: 1,
      maxAttempts: options.maxAttempts ?? 3,
      dedupeKey: options.dedupeKey,
      dedupeScope: options.dedupeKey ? dedupeScope : undefined,
      createdAt: claimedAt,
      // Claimed inline at creation, so it was due the moment it existed.
      runAt: claimedAt,
      claimedAt,
      logs: [],
      claimToken: randomUUID(),
      steps: [],
      journalBytes: 0,
    }

    this.jobs.push(job)

    return this.createHandle(job)
  }

  async claimNext<T>(type: string): Promise<Job<T> | null> {
    // Stand-in for Mongo's unique partial dedupe indexes: a pending job whose
    // key+scope already has an active run is NOT claimable. Without this check
    // the pending-behind-active pair that `dedupeScope: 'pending'` exists to
    // allow got claimed while the first run was still going, so two handlers
    // ran concurrently for one key — the exact failure this backend is supposed
    // to let a unit test catch without booting Mongo.
    //
    // Skips blocked candidates rather than stopping at the first one, so a
    // contended key cannot starve every other key of the same type (see the
    // equivalent reasoning in MongoJobQueue.claimNext).
    const activeSlots = new Set(
      this.jobs
        .filter((j) => j.status === 'active' && j.dedupeKey)
        .map((j) => `${j.dedupeKey}|${j.dedupeScope ?? 'pending+active'}`),
    )
    // `runAt` gate, mirroring Mongo's `{runAt: {$lte: now}}`. Without it a job
    // enqueued with `delay` was claimable instantly here and correctly withheld
    // in production, so a unit test could pass while the same logic misbehaved
    // for real — which defeats the reason this backend exists.
    const now = Date.now()
    const job = this.jobs.find(
      (j) =>
        j.type === type &&
        j.status === 'pending' &&
        j.runAt.getTime() <= now &&
        !(
          j.dedupeKey &&
          activeSlots.has(`${j.dedupeKey}|${j.dedupeScope ?? 'pending+active'}`)
        ),
    )
    if (!job) return null

    this.markClaimed(job)
    return this.toJob(job as RecordedJob<T>, true)
  }

  async claimNextByKey<T>(
    type: string,
    dedupeKey: string,
  ): Promise<JobHandle<T> | null> {
    const now = Date.now()
    const candidates = this.jobs
      .filter(
        (job) =>
          job.type === type &&
          job.dedupeKey === dedupeKey &&
          job.status === 'pending' &&
          job.runAt.getTime() <= now,
      )
      .sort(
        (a, b) =>
          a.priority - b.priority || a.runAt.getTime() - b.runAt.getTime(),
      )
    const job = candidates[0] as RecordedJob<T> | undefined
    if (!job) return null
    const active = this.jobs.some(
      (candidate) =>
        candidate !== job &&
        candidate.status === 'active' &&
        candidate.dedupeKey === dedupeKey &&
        candidate.dedupeScope === job.dedupeScope,
    )
    if (active) return null
    this.markClaimed(job)
    return this.createHandle(job)
  }

  /** Fence check mirroring Mongo's fenced lifecycle filter: with a token the
   * write only applies to an `active` job still holding that token. */
  private fenceMiss(
    job: RecordedJob | undefined,
    claimToken?: string,
  ): boolean {
    if (claimToken === undefined) return false
    return !job || job.status !== 'active' || job.claimToken !== claimToken
  }

  private terminalTransitionMiss(
    job: RecordedJob,
    claimToken?: string,
  ): TerminalWriteMissResult | null {
    if (
      job.status === 'completed' ||
      job.status === 'failed' ||
      job.status === 'superseded'
    ) {
      return { status: 'already-terminal', terminalStatus: job.status }
    }
    if (this.fenceMiss(job, claimToken)) return { status: 'lease-lost' }
    return null
  }

  async complete(
    jobId: string,
    claimToken?: string,
  ): Promise<CompleteJobResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (!job) return { status: 'not-found' }
    if (
      job.status === 'failed' &&
      !job.failedByLifecycleWrite &&
      claimToken !== undefined &&
      job.claimToken === claimToken
    ) {
      job.status = 'completed'
      delete job.failedAt
      delete job.failReason
      return { status: 'completed' }
    }
    const miss = this.terminalTransitionMiss(job, claimToken)
    if (miss) return miss
    job.status = 'completed'
    return { status: 'completed' }
  }

  async fail(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<FailJobResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (!job) return { status: 'not-found' }
    const miss = this.terminalTransitionMiss(job, claimToken)
    if (miss) return miss

    await this.log(jobId, `Failed: ${reason}`)
    const hasPendingFollower =
      job.dedupeKey !== undefined &&
      job.dedupeScope === 'pending' &&
      this.jobs.some(
        (candidate) =>
          candidate.dedupeKey === job.dedupeKey &&
          candidate.dedupeScope === job.dedupeScope &&
          candidate.status === 'pending',
      )
    if (hasPendingFollower) {
      job.status = 'superseded'
      job.failReason = reason
      job.failedAt = new Date()
      return { status: 'superseded' }
    }
    if (job.attempt >= job.maxAttempts) {
      job.status = 'failed'
      job.failReason = reason
      job.failedAt = new Date()
      job.failedByLifecycleWrite = true
      return { status: 'failed-terminal' }
    }
    job.status = 'pending'
    return { status: 'retry-scheduled' }
  }

  async failFatal(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<FailFatalJobResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (!job) return { status: 'not-found' }
    const miss = this.terminalTransitionMiss(job, claimToken)
    if (miss) return miss

    job.status = 'failed'
    job.failedByLifecycleWrite = true
    await this.log(jobId, `Fatal: ${reason}`)
    return { status: 'failed-terminal' }
  }

  async release(
    jobId: string,
    claimToken?: string,
  ): Promise<ReleaseJobResult> {
    const job = this.jobs.find((candidate) => candidate.id === jobId)
    if (!job) return { status: 'not-found' }
    if (
      job.status === 'completed' ||
      job.status === 'failed' ||
      job.status === 'superseded'
    ) {
      return { status: 'already-terminal', terminalStatus: job.status }
    }
    if (
      job.status !== 'active' ||
      (claimToken !== undefined && job.claimToken !== claimToken)
    ) {
      return { status: 'lease-lost' }
    }

    const blocked =
      job.dedupeKey !== undefined &&
      job.dedupeScope !== undefined &&
      this.jobs.some(
        (candidate) =>
          candidate !== job &&
          candidate.status === 'pending' &&
          candidate.dedupeKey === job.dedupeKey &&
          candidate.dedupeScope === job.dedupeScope,
      )
    if (blocked) {
      job.status = 'superseded'
      job.failReason =
        'released: a queued follow-up already covers this work'
      job.failedAt = new Date()
      return { status: 'superseded' }
    }

    job.status = 'pending'
    job.runAt = new Date()
    delete job.claimedAt
    delete job.claimToken
    return { status: 'released' }
  }

  /**
   * Mirrors MongoJobQueue's bounded `logs[]`: newest-N retained, each message
   * clipped, and log bytes kept out of the step-journal total. Log volume must
   * not be able to starve a terminal write here either, or the in-memory
   * suites would certify a wedge that production can still hit.
   */
  async log(jobId: string, message: string): Promise<void> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (!job) return
    job.logs.push(truncateLogMessage(message, this.maxLogMessageBytes))
    if (job.logs.length > this.maxLogEntries) {
      // Oldest first — the tail is what explains a failure. Mongo does this
      // with `$slice: -maxLogEntries` on the push.
      job.logs.splice(0, job.logs.length - this.maxLogEntries)
    }
  }

  /** Extend a live claim's lease while preserving its fence. */
  async heartbeat(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.find((candidate) => candidate.id === jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) job.claimedAt = new Date()
    return 'applied'
  }

  /** Records the call (for reaper wiring assertions); recovers nothing. */
  async recoverStuckJobs(visibilityTimeoutMs?: number): Promise<number> {
    this.recoverStuckJobsCalls.push(visibilityTimeoutMs)
    return 0
  }

  // --- Durable-orchestration journal capability (in-memory) -----------------

  async readSteps(jobId: string): Promise<StepRecord[]> {
    const job = this.jobs.find((j) => j.id === jobId)
    return job ? sortBySeq(job.steps) : []
  }

  async appendStep(
    jobId: string,
    claimToken: string,
    record: StepRecord,
  ): Promise<AppendStepResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    return appendStepInMemory(
      job,
      claimToken,
      record,
      DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
    )
  }

  async completeClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<CompleteClaimedResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    return completeClaimedInMemory(job, claimToken, () => {
      if (job) job.status = 'completed'
    })
  }

  async heartbeatClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<HeartbeatClaimedResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    return heartbeatClaimedInMemory(job, claimToken, () => {
      /* no claimedAt tracked on dummy jobs */
    })
  }

  async hasOutstanding(type: string, dedupeKey: string): Promise<boolean> {
    return this.jobs.some(
      (job) =>
        job.type === type &&
        job.dedupeKey === dedupeKey &&
        (job.status === 'pending' || job.status === 'active'),
    )
  }

  async findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null> {
    const job = this.jobs.find((j) => this.matchesQuery(j, query))
    if (!job) return null

    return {
      id: job.id,
      type: job.type,
      data: job.data as T,
      status: job.status,
      attempt: job.attempt,
      maxAttempts: job.maxAttempts,
      priority: job.priority,
      dedupeKey: job.dedupeKey,
      dedupeScope: job.dedupeScope,
      runAt: job.createdAt,
      createdAt: job.createdAt,
    }
  }

  /** Simple query matching for common patterns used in tests */
  private matchesQuery(
    job: RecordedJob,
    query: Record<string, unknown>,
  ): boolean {
    for (const [key, value] of Object.entries(query)) {
      if (!this.matchesField(job, key, value)) return false
    }
    return true
  }

  private matchesField(job: RecordedJob, key: string, value: unknown): boolean {
    if (key === 'data.userId') {
      const data = job.data as Record<string, unknown> | undefined
      return data?.userId === value
    }
    if (key === 'status') {
      if (typeof value === 'object' && value !== null && '$in' in value) {
        return (value.$in as string[]).includes(job.status)
      }
      return job.status === value
    }
    if (key === 'type') return job.type === value
    return key in job && job[key as keyof RecordedJob] === value
  }

  async getStats(type?: string): Promise<QueueStats> {
    const filtered = type ? this.jobs.filter((j) => j.type === type) : this.jobs
    return {
      pending: filtered.filter((j) => j.status === 'pending').length,
      active: filtered.filter((j) => j.status === 'active').length,
      completed: filtered.filter((j) => j.status === 'completed').length,
      failed: filtered.filter((j) => j.status === 'failed').length,
      superseded: filtered.filter((j) => j.status === 'superseded').length,
      ...backlogAge(filtered),
    }
  }

  async startup(): Promise<void> {
    // No-op
  }

  async shutdown(_timeoutMs?: number): Promise<void> {
    // No-op
  }

  async resetStorage(): Promise<void> {
    this.reset()
  }

  /**
   * Reset all recorded jobs. Call in beforeEach/afterEach.
   */
  reset(): void {
    this.jobs = []
    this.idCounter = 0
    this.recoverStuckJobsCalls = []
  }

  /**
   * Get jobs by type
   */
  getJobsByType(type: string): RecordedJob[] {
    return this.jobs.filter((j) => j.type === type)
  }

  /**
   * Get jobs by status
   */
  getJobsByStatus(status: JobStatus): RecordedJob[] {
    return this.jobs.filter((j) => j.status === status)
  }
}
