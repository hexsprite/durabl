/* eslint-disable max-lines */
/** Executes job handlers synchronously when enqueued. For integration tests. */
import type {
  EnqueueOptions,
  Job,
  JobContext,
  JobHandle,
  JobHandler,
  QueueStats,
} from '../types'

import type { IJobQueueBackend } from './IJobQueueBackend'

export class ImmediateBackend implements IJobQueueBackend {
  private handlers: Map<string, JobHandler<unknown>> = new Map()
  private jobs: Map<string, Job> = new Map()
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
    const job: Job = {
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

    const ctx = this.createContext(job.id, dedupeKey, dedupeScope)
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

    // For claimOrEnqueue, check if any pending exists
    if (options.dedupeKey) {
      // Check for pending job with this dedupeKey
      for (const job of this.jobs.values()) {
        if (job.dedupeKey === options.dedupeKey && job.status === 'pending') {
          return null
        }
      }
    }

    const jobId = this.generateId()
    const job: Job<T> = {
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
    }

    this.jobs.set(jobId, job)

    // Return handle for caller to execute inline
    return {
      id: jobId,
      data,
      complete: async () => {
        await this.complete(jobId)
      },
      fail: async (reason: string) => {
        await this.fail(jobId, reason)
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
        return job as Job<T>
      }
    }
    return null
  }

  private createContext(
    jobId: string,
    dedupeKey: string | undefined,
    dedupeScope: 'pending' | 'pending+active',
  ): JobContext {
    return {
      complete: async () => {
        await this.complete(jobId)
        if (dedupeKey) {
          this.activeDedupeKeys.delete(
            this.getDedupeSetKey(dedupeKey, dedupeScope),
          )
        }
      },
      fail: async (reason: string) => {
        await this.fail(jobId, reason)
        if (dedupeKey) {
          this.activeDedupeKeys.delete(
            this.getDedupeSetKey(dedupeKey, dedupeScope),
          )
        }
      },
      failFatal: async (reason: string) => {
        await this.failFatal(jobId, reason)
        if (dedupeKey) {
          this.activeDedupeKeys.delete(
            this.getDedupeSetKey(dedupeKey, dedupeScope),
          )
        }
      },
      log: (message: string) => {
        void this.log(jobId, message)
      },
      heartbeat: async () => {
        await this.heartbeat(jobId)
      },
    }
  }

  async complete(jobId: string): Promise<void> {
    const job = this.jobs.get(jobId)
    if (job) {
      job.status = 'completed'
      job.completedAt = new Date()
    }
  }

  async fail(jobId: string, reason: string): Promise<void> {
    const job = this.jobs.get(jobId)
    if (job) {
      job.failReason = reason
      if (job.attempt >= job.maxAttempts) {
        job.status = 'failed'
        job.failedAt = new Date()
      } else {
        job.status = 'pending' // Back to pending for retry
      }
    }
  }

  async failFatal(jobId: string, reason: string): Promise<void> {
    const job = this.jobs.get(jobId)
    if (job) {
      job.status = 'failed'
      job.failReason = reason
      job.failedAt = new Date()
    }
  }

  async log(_jobId: string, _message: string): Promise<void> {
    // Could store logs if needed, for now no-op
  }

  async heartbeat(jobId: string): Promise<void> {
    const job = this.jobs.get(jobId)
    if (job) {
      job.claimedAt = new Date()
    }
  }

  async findOne<T>(query: Record<string, unknown>): Promise<Job<T> | null> {
    for (const job of this.jobs.values()) {
      if (this.matchesQuery(job, query)) return job as Job<T>
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
