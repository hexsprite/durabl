/**
 * Executes job handlers synchronously when enqueued. For integration tests.
 *
 * **`delay` is deliberately ignored.** Running the handler inline on enqueue is
 * the entire point of this backend, and honouring a delay would mean either
 * blocking the caller for the duration or deferring to a timer — at which point
 * it is no longer immediate and no longer useful for the "assert the side effect
 * happened" tests it exists to serve. `runAt` is therefore always now.
 *
 * This is a documented divergence from `MongoJobQueue` and `DummyBackend`, both
 * of which withhold a job until it is due. A test that depends on delay
 * semantics wants `DummyBackend` (for claim behaviour) or Mongo (for the real
 * thing); this backend cannot model it honestly.
 */
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
  LifecycleWriteResult,
  ReleaseJobResult,
  TerminalWriteMissResult,
  QueueStats,
  StepRecord,
} from '../types'

import { backlogAge } from './backlogAge'
import {
  registerInlineProcessor,
  type IJobQueueBackend,
  type InlineProcessor,
} from './IJobQueueBackend'

/** Internal job record: public {@link Job} plus the off-public step journal
 *  and its running byte total. Structurally satisfies {@link JournalableJob}. */
type ImmediateJob<T = unknown> = Job<T> & {
  steps: StepRecord[]
  journalBytes: number
  /** Distinguishes a lifecycle failure from a reaper race in `complete()`. */
  failedByLifecycleWrite?: boolean
}

export class ImmediateBackend implements IJobQueueBackend {
  /** Inline backend: runs handlers on enqueue, never via the queue poll loop.
   *  Signals `JobQueue` to refuse orchestrations (which need `process()`). */
  readonly executesInline = true

  private processors: Map<string, InlineProcessor> = new Map()
  private jobs: Map<string, ImmediateJob> = new Map()
  private idCounter = 0
  private activeDedupeKeys: Set<string> = new Set();

  [registerInlineProcessor](type: string, processor: InlineProcessor): void {
    this.processors.set(type, processor)
  }

  private generateId(): string {
    this.idCounter++
    return `immediate-${this.idCounter}`
  }

  private toJob<T>(job: ImmediateJob<T>, includeClaimToken = false): Job<T> {
    const {
      steps: _steps,
      journalBytes: _journalBytes,
      failedByLifecycleWrite: _failedByLifecycleWrite,
      claimToken,
      ...publicJob
    } = job
    return {
      ...publicJob,
      ...(includeClaimToken ? { claimToken } : {}),
    }
  }

  private createHandle<T>(job: ImmediateJob<T>): JobHandle<T> {
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

  private markClaimed(job: ImmediateJob): void {
    job.status = 'active'
    job.attempt++
    job.claimedAt = new Date()
    job.claimToken = randomUUID()
  }

  private getDedupeSetKey(
    dedupeKey: string,
    dedupeScope: 'pending' | 'pending+active',
  ): string {
    return `${dedupeScope}:${dedupeKey}`
  }

  /** Free the reservation after a terminal lifecycle transition. */
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
    const processor = this.processors.get(type)
    if (processor) await processor(this.createHandle(job))

    return jobId
  }

  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options: EnqueueOptions = {},
  ): Promise<JobHandle<T> | null> {
    assertValidLatestCoalescing(options)
    const dedupeScope = options.dedupeScope ?? 'pending+active'

    if (options.dedupeKey) {
      let activeExists = false
      for (const job of this.jobs.values()) {
        if (job.dedupeKey !== options.dedupeKey) continue
        if (job.status === 'pending') {
          if (options.coalesce === 'latest' && job.type === type) {
            job.data = data
          }
          return null
        }
        if (job.status === 'active' && job.dedupeScope === dedupeScope) {
          activeExists = true
        }
      }

      if (activeExists) {
        if (dedupeScope === 'pending') {
          const createdAt = new Date()
          const follower: ImmediateJob<T> = {
            id: this.generateId(),
            type,
            data,
            status: 'pending',
            attempt: 0,
            maxAttempts: options.maxAttempts ?? 3,
            priority: options.priority ?? 0,
            dedupeKey: options.dedupeKey,
            dedupeScope,
            runAt: createdAt,
            createdAt,
            steps: [],
            journalBytes: 0,
          }
          this.jobs.set(follower.id, follower)
        }
        return null
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
    if (job.dedupeKey && job.dedupeScope) {
      this.activeDedupeKeys.add(
        this.getDedupeSetKey(job.dedupeKey, job.dedupeScope),
      )
    }

    return this.createHandle(job)
  }

  async claimNext<T>(type: string): Promise<Job<T> | null> {
    const activeSlots = new Set<string>()
    for (const job of this.jobs.values()) {
      if (job.status === 'active' && job.dedupeKey && job.dedupeScope) {
        activeSlots.add(this.getDedupeSetKey(job.dedupeKey, job.dedupeScope))
      }
    }
    const job = [...this.jobs.values()]
      .filter(
        (candidate) =>
          candidate.type === type &&
          candidate.status === 'pending' &&
          candidate.runAt.getTime() <= Date.now() &&
          !(
            candidate.dedupeKey &&
            candidate.dedupeScope &&
            activeSlots.has(
              this.getDedupeSetKey(
                candidate.dedupeKey,
                candidate.dedupeScope,
              ),
            )
          ),
      )
      .sort(
        (a, b) =>
          a.priority - b.priority || a.runAt.getTime() - b.runAt.getTime(),
      )[0] as ImmediateJob<T> | undefined
    if (!job) return null
    this.markClaimed(job)
    return this.toJob(job, true)
  }

  async claimNextByKey<T>(
    type: string,
    dedupeKey: string,
  ): Promise<JobHandle<T> | null> {
    const job = [...this.jobs.values()]
      .filter(
        (candidate) =>
          candidate.type === type &&
          candidate.dedupeKey === dedupeKey &&
          candidate.status === 'pending' &&
          candidate.runAt.getTime() <= Date.now(),
      )
      .sort(
        (a, b) =>
          a.priority - b.priority || a.runAt.getTime() - b.runAt.getTime(),
      )[0] as ImmediateJob<T> | undefined
    if (!job) return null
    const blocked = [...this.jobs.values()].some(
      (candidate) =>
        candidate !== job &&
        candidate.status === 'active' &&
        candidate.dedupeKey === dedupeKey &&
        candidate.dedupeScope === job.dedupeScope,
    )
    if (blocked) return null
    this.markClaimed(job)
    return this.createHandle(job)
  }



  /** Fence check mirroring Mongo's fenced lifecycle filter: with a token the
   * write only applies to an `active` job still holding that token. */
  private fenceMiss(job: Job | undefined, claimToken?: string): boolean {
    if (claimToken === undefined) return false
    return !job || job.status !== 'active' || job.claimToken !== claimToken
  }

  private terminalTransitionMiss(
    job: Job,
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
    const job = this.jobs.get(jobId)
    if (!job) return { status: 'not-found' }
    if (
      job.status === 'failed' &&
      !job.failedByLifecycleWrite &&
      claimToken !== undefined &&
      job.claimToken === claimToken
    ) {
      job.status = 'completed'
      job.completedAt = new Date()
      delete job.failedAt
      delete job.failReason
      return { status: 'completed' }
    }
    const miss = this.terminalTransitionMiss(job, claimToken)
    if (miss) return miss

    job.status = 'completed'
    job.completedAt = new Date()
    this.releaseDedupeKey(job)
    return { status: 'completed' }
  }

  async fail(
    jobId: string,
    reason: string,
    claimToken?: string,
  ): Promise<FailJobResult> {
    const job = this.jobs.get(jobId)
    if (!job) return { status: 'not-found' }
    const miss = this.terminalTransitionMiss(job, claimToken)
    if (miss) return miss

    job.failReason = reason
    let hasPendingFollower = false
    if (job.dedupeKey && job.dedupeScope === 'pending') {
      for (const candidate of this.jobs.values()) {
        if (
          candidate.dedupeKey === job.dedupeKey &&
          candidate.dedupeScope === job.dedupeScope &&
          candidate.status === 'pending'
        ) {
          hasPendingFollower = true
          break
        }
      }
    }
    if (hasPendingFollower) {
      job.status = 'superseded'
      job.failedAt = new Date()
      this.releaseDedupeKey(job)
      return { status: 'superseded' }
    }
    if (job.attempt >= job.maxAttempts) {
      job.status = 'failed'
      job.failedAt = new Date()
      job.failedByLifecycleWrite = true
      this.releaseDedupeKey(job)
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
    const job = this.jobs.get(jobId)
    if (!job) return { status: 'not-found' }
    const miss = this.terminalTransitionMiss(job, claimToken)
    if (miss) return miss

    job.status = 'failed'
    job.failReason = reason
    job.failedAt = new Date()
    job.failedByLifecycleWrite = true
    this.releaseDedupeKey(job)
    return { status: 'failed-terminal' }
  }

  async release(
    jobId: string,
    claimToken?: string,
  ): Promise<ReleaseJobResult> {
    const job = this.jobs.get(jobId)
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
      [...this.jobs.values()].some(
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
      this.releaseDedupeKey(job)
      return { status: 'superseded' }
    }

    job.status = 'pending'
    job.runAt = new Date()
    delete job.claimedAt
    delete job.claimToken
    return { status: 'released' }
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

  async hasOutstanding(type: string, dedupeKey: string): Promise<boolean> {
    for (const job of this.jobs.values()) {
      if (
        job.type === type &&
        job.dedupeKey === dedupeKey &&
        (job.status === 'pending' || job.status === 'active')
      ) {
        return true
      }
    }
    return false
  }

  async findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null> {
    for (const job of this.jobs.values()) {
      if (this.matchesQuery(job, query)) {
        return this.toJob(job as ImmediateJob<T>)
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
      superseded: jobs.filter((j) => j.status === 'superseded').length,
      ...backlogAge(jobs),
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
    // Processor registration survives storage resets.
  }

}
