/** JobQueue - main API wrapping a backend with processor loop management. */
import { defaultLogger, type Logger } from './logger'

import type { IJobQueueBackend } from './backends/IJobQueueBackend'
import { OrchestrationUnsupportedError } from './journal/errors'
import type {
  AppendStepResult,
  CompleteClaimedResult,
  EnqueueOptions,
  HeartbeatClaimedResult,
  Job,
  JobContext,
  JobHandle,
  JobEvent,
  JobEventSink,
  JobHandler,
  ProcessorConfig,
  QueueStats,
  StepRecord,
} from './types'

/** An in-flight {@link JobQueue.sleep}, tracked so shutdown can cancel it. */
interface PendingSleep {
  timer?: ReturnType<typeof setTimeout>
  resolve: () => void
}

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
/** Default reaper visibility timeout. */
const DEFAULT_VISIBILITY_TIMEOUT_MS = 300000
/** Default cadence for the built-in reaper timer ({@link JobQueue.startReaper}). */
const DEFAULT_REAPER_INTERVAL_MS = 60000
/**
 * Recovered-count at which a sweep is assumed to have hit the backend's batch
 * cap. Mirrors `DEFAULT_REAPER_BATCH_SIZE` in MongoJobQueue; a full batch means
 * there is more waiting, which is the signal worth alerting on.
 */
const REAPER_SATURATION_HINT = 1000

/** Options for the {@link JobQueue} constructor. */
export interface JobQueueOptions {
  /**
   * The reaper visibility timeout (ms) this queue is operated with. Single
   * source of truth (§7.1): the {@link Orchestrator} reads it to size the
   * heartbeat, and {@link JobQueue.startReaper} passes it to the backend's
   * `recoverStuckJobs()` — configure it here, nowhere else.
   * Must be a positive finite number. Default: 300000.
   */
  visibilityTimeoutMs?: number
  /**
   * Metrics sink for lifecycle events. Absent means zero behaviour change and
   * zero cost beyond one undefined check.
   *
   * Logs are for a human reading an incident; this is for a machine counting.
   * Called synchronously and fire-and-forget: a throwing sink is caught and
   * reported through the logger, never allowed to fail a job or take down a
   * worker — same reasoning as the fire-and-forget `ctx.log` write.
   */
  onJobEvent?: JobEventSink
}

export class JobQueue {
  private backend: IJobQueueBackend
  private log: Logger
  private processors: Map<string, ProcessorState> = new Map()
  private isShuttingDown = false
  private unsubscribePush: (() => void) | null = null
  private reaperTimer: ReturnType<typeof setTimeout> | null = null
  /**
   * Whether the reaper loop is live. Separate from {@link reaperTimer} because
   * during an in-flight sweep no timer is armed — without this flag a second
   * `startReaper()` call mid-sweep would start a parallel loop.
   */
  private reaperActive = false
  /** Poll-loop sleeps currently parked; cleared by {@link shutdown}. */
  private pendingSleeps: Set<PendingSleep> = new Set()
  /** In-flight drain started by {@link installSignalHandlers}, if any. */
  private signalDrain: Promise<void> | null = null
  /** Metrics sink, if configured. */
  private onJobEvent?: JobEventSink
  /** Reaper visibility timeout this queue is operated with (§7.1). */
  readonly visibilityTimeoutMs: number

  constructor(
    backend: IJobQueueBackend,
    logger: Logger = defaultLogger,
    options: JobQueueOptions = {},
  ) {
    this.backend = backend
    this.log = logger.child({ category: 'JobQueue' })
    this.onJobEvent = options.onJobEvent
    this.visibilityTimeoutMs =
      options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS
    // A zero/negative window would make the reaper reclaim every active job
    // instantly and the Orchestrator size its heartbeat to setTimeout(0).
    if (
      !Number.isFinite(this.visibilityTimeoutMs) ||
      this.visibilityTimeoutMs <= 0
    ) {
      throw new Error(
        `visibilityTimeoutMs must be a positive finite number of milliseconds, got ${this.visibilityTimeoutMs}`,
      )
    }
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
   *
   * The concurrency slot is reserved *synchronously* (incrementing
   * `activeCount` before the `await backend.claimNext`) and rolled back if no
   * job is claimed. Reserving before yielding is load-bearing: `claimNext`
   * yields, so two concurrent callers (e.g. a burst of push notifications)
   * would otherwise both pass a check-then-await gate and both claim distinct
   * jobs, overshooting the cap on this instance. The atomic per-job claim only
   * stops two workers taking the *same* job — it can't bound one instance's
   * own concurrency.
   *
   * @returns `true` if a job was claimed and handed to `processJob`.
   */
  private async claimAndProcess(state: ProcessorState): Promise<boolean> {
    if (state.activeCount >= state.config.concurrency) return false

    // Reserve the slot now, before any await, so concurrent callers see it.
    state.activeCount++
    try {
      const job = await this.backend.claimNext(state.type)
      if (!job) {
        state.activeCount--
        return false
      }
      // Reset backoff on successful claim. processJob's finally releases the
      // slot we reserved above.
      state.backoffMs = 0
      void this.processJob(state, job)
      return true
    } catch (err) {
      state.activeCount--
      // Log error and apply exponential backoff
      this.log.error({ err, type: state.type }, 'error claiming next job')
      state.backoffMs = Math.min(
        MAX_BACKOFF_MS,
        Math.max(MIN_BACKOFF_MS, state.backoffMs * 2 || MIN_BACKOFF_MS),
      )
      return false
    }
  }

  /** Get queue statistics. */
  async getStats(type?: string): Promise<QueueStats> {
    return this.backend.getStats(type)
  }

  // --- Durable-orchestration journal passthroughs (§3.5) --------------------
  // Thin delegations so an Orchestrator depends only on JobQueue and never
  // reaches around it to the backend. Each throws OrchestrationUnsupportedError
  // if the backend lacks the capability.

  /** Throw if the backend cannot host durable orchestrations — either because
   * it executes jobs inline (bypassing the `process()` loop the wrapper needs)
   * or because it lacks one of the four journal methods. The
   * {@link Orchestrator} constructor calls this. */
  assertJournalCapable(): void {
    // An inline backend runs its own handler registry on enqueue and never
    // drives queue.process() processors, so an orchestration wrapper would
    // never execute — the job would sit 'active' forever. Fail loud instead.
    if (this.backend.executesInline) {
      throw new OrchestrationUnsupportedError(
        'executesInline',
        'backend executes jobs inline on enqueue and never runs queue.process() ' +
          'processors, so it cannot host durable orchestrations — use DummyBackend ' +
          'for orchestration unit tests',
      )
    }
    if (typeof this.backend.readSteps !== 'function') {
      throw new OrchestrationUnsupportedError('readSteps')
    }
    if (typeof this.backend.appendStep !== 'function') {
      throw new OrchestrationUnsupportedError('appendStep')
    }
    if (typeof this.backend.completeClaimed !== 'function') {
      throw new OrchestrationUnsupportedError('completeClaimed')
    }
    if (typeof this.backend.heartbeatClaimed !== 'function') {
      throw new OrchestrationUnsupportedError('heartbeatClaimed')
    }
  }

  async readSteps(jobId: string): Promise<StepRecord[]> {
    if (!this.backend.readSteps) {
      throw new OrchestrationUnsupportedError('readSteps')
    }
    return this.backend.readSteps(jobId)
  }

  async appendStep(
    jobId: string,
    claimToken: string,
    record: StepRecord,
  ): Promise<AppendStepResult> {
    if (!this.backend.appendStep) {
      throw new OrchestrationUnsupportedError('appendStep')
    }
    return this.backend.appendStep(jobId, claimToken, record)
  }

  async completeClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<CompleteClaimedResult> {
    if (!this.backend.completeClaimed) {
      throw new OrchestrationUnsupportedError('completeClaimed')
    }
    return this.backend.completeClaimed(jobId, claimToken)
  }

  async heartbeatClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<HeartbeatClaimedResult> {
    if (!this.backend.heartbeatClaimed) {
      throw new OrchestrationUnsupportedError('heartbeatClaimed')
    }
    return this.backend.heartbeatClaimed(jobId, claimToken)
  }

  /** Initialize the queue (create indexes, etc). */
  async startup(): Promise<void> {
    await this.backend.startup()
  }

  /**
   * Start the stuck-job reaper: periodically invoke the backend's
   * `recoverStuckJobs()` with **this queue's** `visibilityTimeoutMs`, so the
   * value the {@link Orchestrator} sizes heartbeats from and the value the
   * reaper enforces can never drift apart (§7.1). Run this on exactly one
   * process (or accept redundant-but-harmless sweeps on several).
   *
   * The timer is `unref`'d — it won't keep the process alive — and is cleared
   * by {@link stopReaper} / {@link shutdown}.
   *
   * @param intervalMs sweep cadence. Default: 60000.
   * @throws if the backend does not implement `recoverStuckJobs`.
   */
  startReaper(intervalMs = DEFAULT_REAPER_INTERVAL_MS): void {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new Error(
        `startReaper intervalMs must be a positive finite number of milliseconds, got ${intervalMs}`,
      )
    }
    if (typeof this.backend.recoverStuckJobs !== 'function') {
      throw new Error(
        'startReaper: backend does not implement recoverStuckJobs',
      )
    }
    if (this.reaperActive) return // already running — idempotent
    this.reaperActive = true

    // Self-scheduling, not setInterval: the next sweep is armed only once the
    // previous one settles. A setInterval fires on the clock regardless, so a
    // sweep that outlasts its interval — exactly what happens after a mass
    // worker death, when the stuck set is largest — overlapped with the next
    // one and multiplied the cursor work at the worst possible moment.
    // (Same shape as `startHeartbeat` in orchestrator/context.ts.)
    const tick = async (): Promise<void> => {
      if (!this.reaperActive || this.isShuttingDown) return
      try {
        const handled = await this.backend.recoverStuckJobs!(
          this.visibilityTimeoutMs,
        )
        if (handled > 0) {
          this.log.warn({ handled }, 'reaper recovered stuck jobs')
          // `saturated` means the sweep hit its batch cap, so more is waiting —
          // the difference between a blip and a backlog an operator must watch.
          this.emit({
            kind: 'reaper-recovered',
            handled,
            saturated: handled >= REAPER_SATURATION_HINT,
          })
        }
      } catch (err) {
        this.log.error({ err }, 'reaper sweep failed; will retry next tick')
      }
      // stopReaper()/shutdown() may have run while the sweep was in flight.
      if (!this.reaperActive || this.isShuttingDown) return
      this.armReaper(tick, intervalMs)
    }

    this.armReaper(tick, intervalMs)
  }

  /** Arm the next reaper tick. Unref'd — the reaper never keeps a process alive. */
  private armReaper(tick: () => Promise<void>, intervalMs: number): void {
    const timer = setTimeout(() => {
      void tick()
    }, intervalMs)
    timer.unref?.()
    this.reaperTimer = timer
  }

  /** Stop the reaper started by {@link startReaper}. Safe to call twice. */
  stopReaper(): void {
    this.reaperActive = false
    if (this.reaperTimer) {
      clearTimeout(this.reaperTimer)
      this.reaperTimer = null
    }
  }

  /**
   * Drain the queue on process signals, so forgetting to wire shutdown is the
   * loud path rather than the silent one.
   *
   * Skipping the drain is expensive and invisible: in-flight jobs die
   * mid-handler, sit `active` until the visibility timeout expires, burn an
   * attempt each, and re-run their side effects from the top. A deploy is
   * therefore a steady source of duplicated external writes. That is not a
   * hypothetical — the first production consumer of this library had a working
   * DDP graceful-shutdown handler and never called {@link shutdown}, because it
   * was not discoverable at the point where shutdown was being thought about.
   *
   * Not enabled by default: an import must never mutate global process state as
   * a side effect. Call it explicitly.
   *
   * This does NOT call `process.exit()`. Exiting is the host application's
   * decision — a library that exits on your behalf is unusable inside a
   * framework with its own shutdown sequence (Meteor, Next, Nest all have one).
   * Await the returned drain instead, then exit when you are ready.
   *
   * @param opts.signals which signals to handle. Default: SIGTERM, SIGINT.
   * @param opts.timeoutMs drain budget, passed to {@link shutdown}. Must fit
   *   inside your platform's kill deadline, or the platform wins.
   * @returns an uninstall function that removes exactly the listeners this
   *   added — never `removeAllListeners`, which would clobber a host
   *   application's own handlers.
   */
  installSignalHandlers(
    opts: { signals?: NodeJS.Signals[]; timeoutMs?: number } = {},
  ): () => void {
    const signals = opts.signals ?? (['SIGTERM', 'SIGINT'] as NodeJS.Signals[])
    const timeoutMs = opts.timeoutMs
    const registered: Array<[NodeJS.Signals, () => void]> = []

    const onSignal = (signal: NodeJS.Signals) => () => {
      if (this.signalDrain) {
        // A second signal is an operator saying "stop waiting". Honour it by
        // reporting rather than silently starting a parallel drain.
        this.log.warn(
          { signal },
          'second shutdown signal received; drain already in progress',
        )
        return
      }
      this.log.info({ signal, timeoutMs }, 'signal received; draining queue')
      this.signalDrain = this.shutdown(timeoutMs).catch((err: unknown) => {
        // Never let a drain failure become an unhandled rejection that takes
        // the process down harder than the signal already would.
        this.log.error({ err, signal }, 'error draining queue on signal')
      })
    }

    for (const signal of signals) {
      const handler = onSignal(signal)
      process.once(signal, handler)
      registered.push([signal, handler])
    }

    return () => {
      for (const [signal, handler] of registered) {
        process.removeListener(signal, handler)
      }
    }
  }

  /**
   * The in-flight signal-triggered drain, if any. Exposed so a host can await
   * the same drain its signal handler started instead of racing it.
   */
  get draining(): Promise<void> | null {
    return this.signalDrain
  }

  /** Graceful shutdown. Stops processors and waits for active jobs. */
  async shutdown(timeoutMs = 30000): Promise<void> {
    this.isShuttingDown = true

    this.stopReaper()

    // Stop accepting new push notifications immediately
    if (this.unsubscribePush) {
      this.unsubscribePush()
      this.unsubscribePush = null
    }

    // Stop all processors
    for (const state of this.processors.values()) {
      state.running = false
    }

    // Wake every parked poll loop so it exits now instead of at the end of its
    // current interval — and, more importantly, so its timer stops holding the
    // event loop open after this method resolves.
    this.cancelPendingSleeps()

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

      // Fill up to the concurrency limit. claimAndProcess reserves slots
      // synchronously and returns false once there's no job (or the cap is
      // reached, or a claim errored — it sets backoff in that case), so this
      // shares one race-free claim path with push/enqueue-driven pickup.
      while (state.running && !this.isShuttingDown) {
        const claimed = await this.claimAndProcess(state)
        if (!claimed) break
      }
    }
  }

  /**
   * Process a single job
   */
  private async processJob(state: ProcessorState, job: Job): Promise<void> {
    const ctx = this.createContext(job)
    // Claim-to-terminal duration. Measured here rather than from `claimedAt` so
    // it reflects this worker's handling time, not clock skew between hosts.
    const startedAt = Date.now()
    this.emit({
      kind: 'claimed',
      type: job.type,
      jobId: job.id,
      attempt: job.attempt,
    })

    try {
      await state.handler(job, ctx)
      this.emit({
        kind: 'completed',
        type: job.type,
        jobId: job.id,
        durationMs: Date.now() - startedAt,
      })
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err)
      // `terminal` distinguishes a retry from a give-up, which is the whole
      // point of charting this: a flaky job that recovers is not an incident.
      this.emit({
        kind: 'failed',
        type: job.type,
        jobId: job.id,
        durationMs: Date.now() - startedAt,
        attempt: job.attempt,
        maxAttempts: job.maxAttempts,
        terminal: job.attempt >= job.maxAttempts,
        reason,
      })
      try {
        // Fenced with the claim token from *this* claim: if the job was
        // reclaimed (reaper → another worker), the fail must not clobber the
        // live owner's copy. On a fence miss, do nothing and do not retry.
        const res = await this.backend.fail(job.id, reason, job.claimToken)
        if (res === 'lease-lost') this.warnLeaseLost(job.id, 'fail', job.type)
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
   * Hand an event to the configured sink.
   *
   * Wrapped: a throwing sink is a bug in the consumer's metrics code, and it
   * must not be able to fail a job or kill a worker. Reported through the
   * logger so it is visible rather than swallowed.
   */
  private emit(event: JobEvent): void {
    if (!this.onJobEvent) return
    try {
      this.onJobEvent(event)
    } catch (err) {
      this.log.warn({ err, kind: event.kind }, 'onJobEvent sink threw')
    }
  }

  private warnLeaseLost(jobId: string, op: string, type?: string): void {
    this.log.warn(
      { jobId, op },
      'lease lost; skipping fail/complete — job owned by another worker',
    )
    if (type) this.emit({ kind: 'lease-lost', type, jobId, op })
  }

  /**
   * Create JobContext for handler. All lifecycle writes are fenced with the
   * claim token from this claim (when the backend minted one), so a zombie
   * worker can never complete/fail a job that another worker now owns.
   */
  private createContext(job: Job): JobContext {
    const { id: jobId, claimToken, type } = job
    return {
      complete: async () => {
        const res = await this.backend.complete(jobId, claimToken)
        if (res === 'lease-lost') this.warnLeaseLost(jobId, 'complete', type)
      },
      fail: async (reason: string) => {
        const res = await this.backend.fail(jobId, reason, claimToken)
        if (res === 'lease-lost') this.warnLeaseLost(jobId, 'fail', type)
      },
      failFatal: async (reason: string) => {
        const res = await this.backend.failFatal(jobId, reason, claimToken)
        if (res === 'lease-lost') this.warnLeaseLost(jobId, 'failFatal', type)
        else this.emit({ kind: 'fail-fatal', type, jobId, reason })
      },
      log: (message: string) => {
        // Fire-and-forget, but never unhandled: a rejected job-log write with
        // no catch takes the whole worker down under Node's default
        // --unhandled-rejections=throw. A lost log line is not worth a
        // process; report it and carry on.
        this.backend.log(jobId, message).catch((err: unknown) => {
          this.log.warn({ err, jobId }, 'failed to write job log entry')
        })
      },
      heartbeat: async () => {
        const res = await this.backend.heartbeat(jobId, claimToken)
        if (res === 'lease-lost') this.warnLeaseLost(jobId, 'heartbeat', type)
        return res
      },
    }
  }

  /**
   * Sleep that shutdown can cut short.
   *
   * The poll loops park here for a full `pollInterval` between sweeps — 60s
   * when push is active. A plain `setTimeout` left `shutdown()` resolving
   * while an armed timer still held the event loop open, so a process that
   * awaited a graceful shutdown sat there for up to a minute before exiting.
   * Registering the timer lets {@link cancelPendingSleeps} clear it and wake
   * the loop, which then sees the shutdown flag and returns.
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const entry: PendingSleep = { resolve }
      entry.timer = setTimeout(() => {
        this.pendingSleeps.delete(entry)
        resolve()
      }, ms)
      this.pendingSleeps.add(entry)
    })
  }

  /** Wake every parked poll loop at once. Each re-checks its run flags and exits. */
  private cancelPendingSleeps(): void {
    for (const entry of this.pendingSleeps) {
      if (entry.timer) clearTimeout(entry.timer)
      entry.resolve()
    }
    this.pendingSleeps.clear()
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
