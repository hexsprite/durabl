/* eslint-disable max-lines */
/**
 * DummyBackend - For Unit Tests
 *
 * Records all operations without executing handlers.
 * Use to verify job creation, dedupe behavior, and arguments.
 */

import type {
  EnqueueOptions,
  Job,
  JobHandle,
  JobStatus,
  QueueStats,
} from '../types'

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
  logs: string[]
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
      createdAt: new Date(),
      logs: [],
    }

    this.jobs.push(job)
    return job.id
  }

  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options: EnqueueOptions = {},
  ): Promise<JobHandle<T> | null> {
    const dedupeScope = options.dedupeScope ?? 'pending+active'

    // Check for existing pending job
    if (options.dedupeKey) {
      const existing = this.jobs.find(
        (job) =>
          job.dedupeKey === options.dedupeKey && job.status === 'pending',
      )
      if (existing) {
        return null
      }
    }

    // Create and immediately claim
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
      createdAt: new Date(),
      logs: [],
    }

    this.jobs.push(job)

    return {
      id: job.id,
      data,
      complete: async () => {
        job.status = 'completed'
      },
      fail: async (reason: string) => {
        job.status = 'failed'
        job.logs.push(`Failed: ${reason}`)
      },
      log: (message: string) => {
        job.logs.push(message)
      },
    }
  }

  async claimNext<T>(type: string): Promise<Job<T> | null> {
    const job = this.jobs.find((j) => j.type === type && j.status === 'pending')
    if (!job) return null

    job.status = 'active'
    job.attempt++

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

  async complete(jobId: string): Promise<void> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (job) {
      job.status = 'completed'
    }
  }

  async fail(jobId: string, reason: string): Promise<void> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (job) {
      job.logs.push(`Failed: ${reason}`)
      if (job.attempt >= job.maxAttempts) {
        job.status = 'failed'
      } else {
        job.status = 'pending' // Back to pending for retry
      }
    }
  }

  async failFatal(jobId: string, reason: string): Promise<void> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (job) {
      job.status = 'failed'
      job.logs.push(`Fatal: ${reason}`)
    }
  }

  async log(jobId: string, message: string): Promise<void> {
    const job = this.jobs.find((j) => j.id === jobId)
    if (job) {
      job.logs.push(message)
    }
  }

  async heartbeat(_jobId: string): Promise<void> {
    // No-op for dummy backend
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
