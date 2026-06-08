/** JobQueue - main API wrapping a backend with processor loop management. */
import { defaultLogger, type Logger } from './logger'

import type { IJobQueueBackend } from './backends/IJobQueueBackend'
import type {
  EnqueueOptions,
  Job,
  JobContext,
  JobHandle,
  JobHandler,
  ProcessorConfig,
  QueueStats,
} from './types'

interface ProcessorState {
  type: string
  handler: JobHandler<unknown>
  config: Required<ProcessorConfig>
  running: boolean
  activeCount: number
  /** Current backoff delay after errors (resets on success) */
  backoffMs: number
}

const MIN_BACKOFF_MS = 1000
const MAX_BACKOFF_MS = 60000
const DEFAULT_POLL_INTERVAL_MS = 5000
/** Safety-net poll interval when backend pushes new-job notifications. */
const PUSH_POLL_INTERVAL_MS = 60000

export class JobQueue {
  private backend: IJobQueueBackend
  private log: Logger
  private processors: Map<string, ProcessorState> = new Map()
  private isShuttingDown = false
  private unsubscribePush: (() => void) | null = null

  constructor(backend: IJobQueueBackend, logger: Logger = defaultLogger) {
    this.backend = backend
    this.log = logger.child({ category: 'JobQueue' })
    // A backend may implement onJobAvailable but return null when push is
    // currently disabled (MongoJobQueue w/ change streams flag off). Only
    // treat it as push-capable when we get a live unsubscribe back.
    const unsub = backend.onJobAvailable?.((t) => this.tryProcessNext(t))
    if (unsub) this.unsubscribePush = unsub
  }

  /** Add a job. Returns job ID, or null if dedupe prevented creation. */
  async enqueue(
    type: string,
    data: unknown,
    options?: EnqueueOptions,
  ): Promise<string | null> {
    const jobId = await this.backend.enqueue(type, data, options)
    // If a local processor has capacity, try to run immediately.
    if (jobId) this.tryProcessNext(type)
    return jobId
  }

  /** Atomically create+claim for inline execution (coalescing pattern). */
  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options?: EnqueueOptions,
  ): Promise<JobHandle<T> | null> {
    return this.backend.claimOrEnqueue(type, data, options)
  }

  /** Register a job processor for a type. Starts a polling loop. */
  process<T>(
    type: string,
    handler: JobHandler<T>,
    config: ProcessorConfig = {},
  ): void {
    if (this.processors.has(type)) {
      throw new Error(`Processor already registered for type: ${type}`)
    }

    const defaultPollInterval = this.unsubscribePush
      ? PUSH_POLL_INTERVAL_MS
      : DEFAULT_POLL_INTERVAL_MS

    const state: ProcessorState = {
      type,
      handler: handler as JobHandler<unknown>,
      config: {
        concurrency: config.concurrency ?? 1,
        pollInterval: config.pollInterval ?? defaultPollInterval,
      },
      running: true,
      activeCount: 0,
      backoffMs: 0,
    }

    this.processors.set(type, state)
    void this.startProcessorLoop(state)
  }

  /**
   * Try to process next job if processor has capacity. Called after
   * enqueue, after job completion, and on push notifications.
   *
   * Empty-string `type` is a catch-up sentinel used by push backends (e.g.
   * `MongoChangeStreamWatcher` after a reconnect): "any processor may have
   * missed a job — try them all". Without it, reconnect pickup latency
   * would fall back to the safety-net poll.
   */
  private tryProcessNext(type: string): void {
    if (this.isShuttingDown) return
    if (type === '') {
      for (const state of this.processors.values()) {
        if (state.running && state.activeCount < state.config.concurrency) {
          void this.claimAndProcess(state)
        }
      }
      return
    }
    const state = this.processors.get(type)
    if (!state || !state.running) return
    if (state.activeCount >= state.config.concurrency) return
    void this.claimAndProcess(state)
  }

  /**
   * Claim next job and process it if available.
   * Wraps backend calls in try/catch to prevent processor death on transient errors.
   */
  private async claimAndProcess(state: ProcessorState): Promise<void> {
    if (state.activeCount >= state.config.concurrency) return

    try {
      const job = await this.backend.claimNext(state.type)
      if (!job) return

      // Reset backoff on successful claim
      state.backoffMs = 0
      state.activeCount++
      void this.processJob(state, job)
    } catch (err) {
      // Log error and apply exponential backoff
      this.log.error({ err, type: state.type }, 'error claiming next job')
      state.backoffMs = Math.min(
        MAX_BACKOFF_MS,
        Math.max(MIN_BACKOFF_MS, state.backoffMs * 2 || MIN_BACKOFF_MS),
      )
    }
  }

  /** Get queue statistics. */
  async getStats(type?: string): Promise<QueueStats> {
    return this.backend.getStats(type)
  }

  /** Initialize the queue (create indexes, etc). */
  async startup(): Promise<void> {
    await this.backend.startup()
  }

  /** Graceful shutdown. Stops processors and waits for active jobs. */
  async shutdown(timeoutMs = 30000): Promise<void> {
    this.isShuttingDown = true

    // Stop accepting new push notifications immediately
    if (this.unsubscribePush) {
      this.unsubscribePush()
      this.unsubscribePush = null
    }

    // Stop all processors
    for (const state of this.processors.values()) {
      state.running = false
    }

    // Wait for active jobs to complete
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      let activeCount = 0
      for (const state of this.processors.values()) {
        activeCount += state.activeCount
      }
      if (activeCount === 0) break
      await this.sleep(100)
    }

    await this.backend.shutdown(timeoutMs)
  }

  /**
   * Background poll loop - catches jobs missed by inline processing.
   * Runs at pollInterval as fallback (other servers, crash recovery).
   * Includes error handling with exponential backoff.
   */
  private async startProcessorLoop(state: ProcessorState): Promise<void> {
    while (state.running && !this.isShuttingDown) {
      // Wait for poll interval (or backoff if in error state)
      const waitTime = state.backoffMs || state.config.pollInterval
      await this.sleep(waitTime)

      // Try to fill up to concurrency limit
      try {
        while (state.activeCount < state.config.concurrency && state.running) {
          const job = await this.backend.claimNext(state.type)
          if (!job) break

          // Reset backoff on successful claim
          state.backoffMs = 0
          state.activeCount++
          void this.processJob(state, job)
        }
      } catch (err) {
        // Log error and apply exponential backoff
        this.log.error({ err, type: state.type }, 'processor loop error')
        state.backoffMs = Math.min(
          MAX_BACKOFF_MS,
          Math.max(MIN_BACKOFF_MS, state.backoffMs * 2 || MIN_BACKOFF_MS),
        )
      }
    }
  }

  /**
   * Process a single job
   */
  private async processJob(state: ProcessorState, job: Job): Promise<void> {
    const ctx = this.createContext(job.id)

    try {
      await state.handler(job, ctx)
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err)
      try {
        await this.backend.fail(job.id, reason)
      } catch (failErr) {
        this.log.error({ failErr, jobId: job.id }, 'error marking job as failed')
      }
    } finally {
      state.activeCount--
      // Try to pick up next job now that we have capacity
      this.tryProcessNext(state.type)
    }
  }

  /**
   * Create JobContext for handler
   */
  private createContext(jobId: string): JobContext {
    return {
      complete: () => this.backend.complete(jobId),
      fail: (reason: string) => this.backend.fail(jobId, reason),
      failFatal: (reason: string) => this.backend.failFatal(jobId, reason),
      log: (message: string) => {
        void this.backend.log(jobId, message)
      },
      heartbeat: () => this.backend.heartbeat(jobId),
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }
}

// Global Backend Management

let globalBackend: IJobQueueBackend | null = null
let defaultQueue: JobQueue | null = null

/** Set the global backend (call in startup or test setup). */
export function setGlobalBackend(backend: IJobQueueBackend): void {
  globalBackend = backend
  defaultQueue = new JobQueue(backend)
}

/** Get the global backend. */
export function getGlobalBackend(): IJobQueueBackend | null {
  return globalBackend
}

/** Get the default queue instance. */
export function getDefaultQueue(): JobQueue {
  if (!defaultQueue) {
    throw new Error('JobQueue not initialized. Call setGlobalBackend() first.')
  }
  return defaultQueue
}

/** Create a new JobQueue with the global backend. */
export function createJobQueue(): JobQueue {
  if (!globalBackend) {
    throw new Error('JobQueue backend not set. Call setGlobalBackend() first.')
  }
  return new JobQueue(globalBackend)
}
