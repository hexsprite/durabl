/** Executes job handlers synchronously when enqueued. For integration tests. */
import { randomUUID } from 'node:crypto'

import {
  appendStepInMemory,
  completeClaimedInMemory,
  heartbeatClaimedInMemory,
  type JournalableJob,
} from '../journal/inMemory'
import {
  DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
  sortBySeq,
} from '../journal/serialize'
import type {
  AppendStepResult,
  CompleteClaimedResult,
  EnqueueOptions,
  HeartbeatClaimedResult,
  Job,
  JobContext,
  JobHandle,
  JobHandler,
  LifecycleWriteResult,
  QueueStats,
  StepRecord,
} from '../types'

import type { IJobQueueBackend } from './IJobQueueBackend'

/** Internal job record: public {@link Job} plus the off-public step journal
 *  and its running byte total. Structurally satisfies {@link JournalableJob}. */
type ImmediateJob<T = unknown> = Job<T> & {
  steps: StepRecord[]
  journalBytes: number
}

export class ImmediateBackend implements IJobQueueBackend {
  /** Inline backend: runs handlers on enqueue, never via the queue poll loop.
   *  Signals `JobQueue` to refuse orchestrations (which need `process()`). */
  readonly executesInline = true

  private handlers: Map<string, JobHandler<unknown>> = new Map()
  private jobs: Map<string, ImmediateJob> = new Map()
  private idCounter = 0
  private activeDedupeKeys: Set<string> = new Set()

  /** Register a handler for a job type. Must be called before enqueue(). */
  registerHandler<T>(type: string, handler: JobHandler<T>): void {
    this.handlers.set(type, handler as JobHandler<unknown>)
  }

  private generateId(): string {
    this.idCounter++
    return `immediate-${this.idCounter}`
  }

  private getDedupeSetKey(
    dedupeKey: string,
    dedupeScope: 'pending' | 'pending+active',
  ): string {
    return `${dedupeScope}:${dedupeKey}`
  }

  /** Free a job's dedupe reservation. `enqueue` reserves the key up front, so
   *  every terminal transition must release it or the key blocks all future
   *  enqueues forever. The inline `createContext` path frees it directly; the
   *  orchestrator completion path (`completeClaimed`) must do the same. */
  private releaseDedupeKey(job: Job): void {
    if (!job.dedupeKey || !job.dedupeScope) return
    this.activeDedupeKeys.delete(
      this.getDedupeSetKey(job.dedupeKey, job.dedupeScope),
    )
  }

  async enqueue(
    type: string,
    data: unknown,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    const dedupeScope = options.dedupeScope ?? 'pending+active'

    // Check for duplicate
    if (options.dedupeKey) {
      const setKey = this.getDedupeSetKey(options.dedupeKey, dedupeScope)
      if (this.activeDedupeKeys.has(setKey)) {
        return null
      }
      this.activeDedupeKeys.add(setKey)
    }

    const jobId = this.generateId()
    const job: ImmediateJob = {
      id: jobId,
      type,
      data,
      status: 'active',
      attempt: 1,
      maxAttempts: options.maxAttempts ?? 3,
      priority: options.priority ?? 0,
      dedupeKey: options.dedupeKey,
      dedupeScope: options.dedupeKey ? dedupeScope : undefined,
      runAt: new Date(),
      createdAt: new Date(),
      claimedAt: new Date(),
      claimToken: randomUUID(),
      steps: [],
      journalBytes: 0,
    }

    this.jobs.set(jobId, job)
    await this.executeHandler(job, options.dedupeKey, dedupeScope)

    return jobId
  }

  private async executeHandler(
    job: Job,
    dedupeKey: string | undefined,
    dedupeScope: 'pending' | 'pending+active',
  ): Promise<void> {
    const handler = this.handlers.get(job.type)
    if (!handler) return

    const ctx = this.createContext(job.id, dedupeKey, dedupeScope, job.claimToken)
    try {
      await handler(job, ctx)
    } catch (err) {
      this.handleExecutionError(job.id, dedupeKey, dedupeScope, err)
    }
  }

  private handleExecutionError(
    jobId: string,
    dedupeKey: string | undefined,
    dedupeScope: 'pending' | 'pending+active',
    err: unknown,
  ): void {
    const storedJob = this.jobs.get(jobId)
    if (storedJob && storedJob.status === 'active') {
      storedJob.status = 'failed'
      storedJob.failReason = err instanceof Error ? err.message : String(err)
    }
    if (dedupeKey) {
      const key = this.getDedupeSetKey(dedupeKey, dedupeScope)
      this.activeDedupeKeys.delete(key)
    }
  }

  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options: EnqueueOptions = {},
  ): Promise<JobHandle<T> | null> {
    const dedupeScope = options.dedupeScope ?? 'pending+active'

    if (options.dedupeKey) {
      for (const job of this.jobs.values()) {
        if (job.dedupeKey !== options.dedupeKey) continue
        // A run is already queued — don't start another now.
        if (job.status === 'pending') return null
        // Stand-in for Mongo's unique partial indexes: at most one active run
        // per key+scope. Without this an un-completed handle did not stop the
        // next caller from getting one too.
        if (job.status === 'active' && job.dedupeScope === dedupeScope) {
          // Deliberate divergence from MongoJobQueue: it queues one follow-up
          // here. This backend executes on enqueue, so "queue a follow-up"
          // would mean running the very job we are coalescing away, inline and
          // immediately. Returning null is the honest inline equivalent.
          return null
        }
      }
    }

    const jobId = this.generateId()
    const job: ImmediateJob<T> = {
      id: jobId,
      type,
      data,
      status: 'active',
      attempt: 1,
      maxAttempts: options.maxAttempts ?? 3,
      priority: options.priority ?? 0,
      dedupeKey: options.dedupeKey,
      dedupeScope: options.dedupeKey ? dedupeScope : undefined,
      runAt: new Date(),
      createdAt: new Date(),
      claimedAt: new Date(),
      claimToken: randomUUID(),
      steps: [],
      journalBytes: 0,
    }

    this.jobs.set(jobId, job)

    // Fence the handle with the claim token minted above, mirroring
    // MongoJobQueue.createHandle: a stale handle whose job was reclaimed by
    // another worker (new token) becomes a no-op instead of clobbering it.
    const { claimToken } = job

    // Return handle for caller to execute inline
    return {
      id: jobId,
      data,
      complete: async () => {
        await this.complete(jobId, claimToken)
      },
      fail: async (reason: string) => {
        await this.fail(jobId, reason, claimToken)
      },
      log: (message: string) => {
        void this.log(jobId, message)
      },
    }
  }

  async claimNext<T>(type: string): Promise<Job<T> | null> {
    for (const job of this.jobs.values()) {
      if (job.type === type && job.status === 'pending') {
        job.status = 'active'
        job.attempt++
        job.claimedAt = new Date()
        job.claimToken = randomUUID()
        return job as Job<T>
      }
    }
    return null
  }

  private createContext(
    jobId: string,
    dedupeKey: string | undefined,
    dedupeScope: 'pending' | 'pending+active',
    claimToken?: string,
  ): JobContext {
    return {
      complete: async () => {
        await this.complete(jobId, claimToken)
        if (dedupeKey) {
          this.activeDedupeKeys.delete(
            this.getDedupeSetKey(dedupeKey, dedupeScope),
          )
        }
      },
      fail: async (reason: string) => {
        await this.fail(jobId, reason, claimToken)
        if (dedupeKey) {
          this.activeDedupeKeys.delete(
            this.getDedupeSetKey(dedupeKey, dedupeScope),
          )
        }
      },
      failFatal: async (reason: string) => {
        await this.failFatal(jobId, reason, claimToken)
        if (dedupeKey) {
          this.activeDedupeKeys.delete(
            this.getDedupeSetKey(dedupeKey, dedupeScope),
          )
        }
      },
      log: (message: string) => {
        // Safe to leave uncaught: this backend's `log` is a synchronous
        // in-memory byte-count update and has no failure mode. The Mongo path
        // needs a catch (see JobQueue.createContext); this one does not.
        void this.log(jobId, message)
      },
      heartbeat: () => this.heartbeat(jobId, claimToken),
    }
  }

  /** Fence check mirroring Mongo's fenced lifecycle filter: with a token the
   * write only applies to an `active` job still holding that token. */
  private fenceMiss(job: Job | undefined, claimToken?: string): boolean {
    if (!claimToken) return false
    return !job || job.status !== 'active' || job.claimToken !== claimToken
  }

  async complete(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.get(jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) {
      job.status = 'completed'
      job.completedAt = new Date()
    }
    return 'applied'
  }

  async fail(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.get(jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) {
      job.failReason = reason
      if (job.attempt >= job.maxAttempts) {
        job.status = 'failed'
        job.failedAt = new Date()
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
    const job = this.jobs.get(jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) {
      job.status = 'failed'
      job.failReason = reason
      job.failedAt = new Date()
    }
    return 'applied'
  }

  /**
   * This backend keeps no log array, and log bytes no longer count against the
   * step-journal budget — `logs[]` is bounded structurally in the backends that
   * persist it (see `MongoJobQueue.logWrite`) so a chatty handler can neither
   * wedge a job at Mongo's document cap nor trip a spurious `JournalTooLarge`.
   * Nothing to do here.
   */
  async log(_jobId: string, _message: string): Promise<void> {}

  async heartbeat(
    jobId: string,
    claimToken?: string,
  ): Promise<LifecycleWriteResult> {
    const job = this.jobs.get(jobId)
    if (this.fenceMiss(job, claimToken)) return 'lease-lost'
    if (job) {
      job.claimedAt = new Date()
    }
    return 'applied'
  }

  // --- Durable-orchestration journal capability (in-memory) -----------------

  /** The stored job IS the journal view — the helpers mutate `steps` and
   *  `journalBytes` in place, so no adapter copy is allowed here. */
  private journalView(jobId: string): JournalableJob | undefined {
    return this.jobs.get(jobId)
  }

  async readSteps(jobId: string): Promise<StepRecord[]> {
    const job = this.jobs.get(jobId)
    return job ? sortBySeq(job.steps) : []
  }

  async appendStep(
    jobId: string,
    claimToken: string,
    record: StepRecord,
  ): Promise<AppendStepResult> {
    return appendStepInMemory(
      this.journalView(jobId),
      claimToken,
      record,
      DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
    )
  }

  async completeClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<CompleteClaimedResult> {
    return completeClaimedInMemory(this.journalView(jobId), claimToken, () => {
      const job = this.jobs.get(jobId)
      if (job) {
        job.status = 'completed'
        job.completedAt = new Date()
        // A reaper-failed-but-actually-complete run flips back — clear the
        // stale failure markers, mirroring MongoJobQueue.completeClaimed.
        delete job.failReason
        delete job.failedAt
        // enqueue() reserved the dedupe key; completion must free it or a
        // later enqueue with that key is blocked forever.
        this.releaseDedupeKey(job)
      }
    })
  }

  async heartbeatClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<HeartbeatClaimedResult> {
    return heartbeatClaimedInMemory(this.journalView(jobId), claimToken, () => {
      const job = this.jobs.get(jobId)
      if (job) job.claimedAt = new Date()
    })
  }

  async findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null> {
    for (const job of this.jobs.values()) {
      if (this.matchesQuery(job, query)) {
        // General read view: strip the live fencing claimToken (and internal
        // journal fields) — only the claim path may see the token.
        const {
          claimToken: _claimToken,
          steps: _steps,
          journalBytes: _journalBytes,
          ...publicView
        } = job
        return publicView as Job<T>
      }
    }
    return null
  }

  /** Simple query matching for common patterns used in tests */
  private matchesQuery(job: Job, query: Record<string, unknown>): boolean {
    for (const [key, value] of Object.entries(query)) {
      if (!this.matchesField(job, key, value)) return false
    }
    return true
  }

  private matchesField(job: Job, key: string, value: unknown): boolean {
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
    return key in job && job[key as keyof Job] === value
  }

  async getStats(type?: string): Promise<QueueStats> {
    let jobs = Array.from(this.jobs.values())
    if (type) {
      jobs = jobs.filter((j) => j.type === type)
    }
    return {
      pending: jobs.filter((j) => j.status === 'pending').length,
      active: jobs.filter((j) => j.status === 'active').length,
      completed: jobs.filter((j) => j.status === 'completed').length,
      failed: jobs.filter((j) => j.status === 'failed').length,
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
   * Reset all state. Call in beforeEach/afterEach.
   */
  reset(): void {
    this.jobs.clear()
    this.activeDedupeKeys.clear()
    this.idCounter = 0
    // Note: handlers are NOT cleared - they're typically set up once
  }

  /**
   * Clear registered handlers
   */
  clearHandlers(): void {
    this.handlers.clear()
  }
}
