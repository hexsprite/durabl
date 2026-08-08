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
import type {
  AppendStepResult,
  CompleteClaimedResult,
  EnqueueOptions,
  HeartbeatClaimedResult,
  Job,
  JobHandle,
  JobStatus,
  LifecycleWriteResult,
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
  /**
   * When the job becomes due (`createdAt + delay`). Recorded so `getStats`
   * can report backlog age the same way the Mongo backend does.
   *
   * NOTE: `claimNext` here does NOT filter on it — a delayed job is claimable
   * immediately in this backend, unlike Mongo. That divergence predates this
   * field and is tracked separately; see du-dum (shared InMemoryJobStore).
   */
  runAt: Date
  logs: string[]
  claimToken?: string
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
    const dedupeScope = options.dedupeScope ?? 'pending+active'

    if (options.dedupeKey) {
      // A run is already queued — don't start another now.
      const pending = this.jobs.find(
        (job) =>
          job.dedupeKey === options.dedupeKey && job.status === 'pending',
      )
      if (pending) return null

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
      logs: [],
      claimToken: randomUUID(),
      steps: [],
      journalBytes: 0,
    }

    this.jobs.push(job)

    // Fence the handle with the token minted above, mirroring
    // MongoJobQueue.createHandle: once the job is reclaimed (new token), a
    // stale handle's complete/fail is a no-op and cannot clobber the new owner.
    const { claimToken } = job

    return {
      id: job.id,
      data,
      complete: async () => {
        if (this.fenceMiss(job, claimToken)) return
        job.status = 'completed'
      },
      fail: async (reason: string) => {
        if (this.fenceMiss(job, claimToken)) return
        job.status = 'failed'
        job.logs.push(`Failed: ${reason}`)
      },
      log: (message: string) => {
        job.logs.push(message)
      },
    }
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

    job.status = 'active'
    job.attempt++
    job.claimToken = randomUUID()

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
      runAt: job.runAt,
      createdAt: job.createdAt,
      claimToken: job.claimToken,
    }
  }

  /** Fence check mirroring Mongo's fenced lifecycle filter: with a token the
   * write only applies to an `active` job still holding that token. */
  private fenceMiss(
    job: RecordedJob | undefined,
    claimToken?: string,
  ): boolean {
    if (!claimToken) return false
    return !job || job.status !== 'active' || job.claimToken !== claimToken
  }

  async complete(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) {
      job.status = 'completed'
    }
    return 'applied'
  }

  async fail(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) {
      job.logs.push(`Failed: ${reason}`)
      if (job.attempt >= job.maxAttempts) {
        job.status = 'failed'
      } else {
        job.status = 'pending' // Back to pending for retry
      }
    }
    return 'applied'
  }

  async failFatal(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) {
      job.status = 'failed'
      job.logs.push(`Fatal: ${reason}`)
    }
    return 'applied'
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

  /**
   * Dummy jobs track no `claimedAt`, so there is nothing to extend — but the
   * lease fence is still modelled, or a test could not tell a live heartbeat
   * from a zombie worker's.
   */
  async heartbeat(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
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
