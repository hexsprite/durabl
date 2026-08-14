/** JobQueue - main API wrapping a backend with processor loop management. */
import { defaultLogger, type Logger } from './logger'

import {
  registerInlineProcessor,
  type IJobQueueBackend,
} from './backends/IJobQueueBackend'
import {
  LeaseLostError,
  OrchestrationUnsupportedError,
} from './journal/errors'
import { assertValidLatestCoalescing, FatalJobError } from './types'
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
  RunClaimedOptions,
  StartReaperResult,
} from './types'

type InlineSlotWaiter = (acquired: boolean) => void

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
  inlineSlotWaiters: InlineSlotWaiter[]
  /** Current backoff delay after errors (resets on success) */
  backoffMs: number
}

interface ManagedRun {
  handle: JobHandle<unknown>
  controller: AbortController
  stopHeartbeat: () => void
  settled: Promise<void>
  resolveSettled: () => void
}

type ManagedExecutionResult =
  | { status: 'completed' }
  | { status: 'superseded'; handlerError: unknown }
  | { status: 'stopped' }

const MIN_BACKOFF_MS = 1000
const MAX_BACKOFF_MS = 60000
const DEFAULT_POLL_INTERVAL_MS = 5000
/** Safety-net poll interval when backend pushes new-job notifications. */
const PUSH_POLL_INTERVAL_MS = 60000
/** Default reaper visibility timeout. */
const DEFAULT_VISIBILITY_TIMEOUT_MS = 300000
/** Default cadence for the built-in reaper timer ({@link JobQueue.startReaper}). */
const DEFAULT_REAPER_INTERVAL_MS = 60000
const DEFAULT_MAX_DRAINS = 10
/** Maximum wait for all post-abort release writes during shutdown. */
const SHUTDOWN_RELEASE_WAIT_MS = 1000
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

/** Options captured with the global backend. */
export type GlobalQueueOptions = JobQueueOptions & { logger?: Logger }

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
  /** Shared by callers while the immediate startup sweep is in flight. */
  private reaperStarting: Promise<StartReaperResult> | null = null
  /** Poll-loop sleeps currently parked; cleared by {@link shutdown}. */
  private pendingSleeps: Set<PendingSleep> = new Set()
  /** In-flight drain started by {@link installSignalHandlers}, if any. */
  private signalDrain: Promise<void> | null = null
  /** Claims currently owned by queue-managed handler execution. */
  private managedRuns: Set<ManagedRun> = new Set()
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
    // Inline backends already awaited their registered managed callback.
    if (jobId && !this.backend.executesInline) this.tryProcessNext(type)
    return jobId
  }

  /** Atomically create+claim for inline execution (coalescing pattern). */
  async claimOrEnqueue<T>(
    type: string,
    data: T,
    options?: EnqueueOptions,
  ): Promise<JobHandle<T> | null> {
    assertValidLatestCoalescing(options)
    this.assertRunAdmission()
    const handle = await this.backend.claimOrEnqueue(type, data, options)
    if (handle && !this.runAdmissionOpen()) {
      await this.rejectClaimAfterShutdown(handle)
    }
    return handle
  }

  /**
   * Execute a live claim under queue-owned lifecycle management.
   *
   * A successful return or superseded failure can claim one pending follower
   * with the same key. The chain preserves and later throws the first
   * superseded handler error after newer work runs.
   */
  async runClaimed<T>(
    initial: JobHandle<T>,
    handler: JobHandler<T>,
    options: RunClaimedOptions = {},
  ): Promise<void> {
    if (!this.runAdmissionOpen()) {
      await this.rejectClaimAfterShutdown(initial)
    }
    const maxDrains = options.maxDrains ?? DEFAULT_MAX_DRAINS
    this.assertValidMaxDrains(maxDrains)
    await this.executeManagedChain(initial, handler, maxDrains)
  }

  /**
   * Register a managed processor. Inline backends invoke it during enqueue;
   * other backends start a polling loop.
   */
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
        heartbeatIntervalMs:
          config.heartbeatIntervalMs ??
          Math.max(1, Math.floor(this.visibilityTimeoutMs / 3)),
      },
      running: true,
      activeCount: 0,
      backoffMs: 0,
      inlineSlotWaiters: [],
    }

    const register = this.backend[registerInlineProcessor]
    if (this.backend.executesInline && !register) {
      throw new Error('inline backend does not expose managed processor registration')
    }

    this.processors.set(type, state)
    if (register) {
      register.call(this.backend, type, async (job) => {
        const acquired = await this.acquireInlineSlot(state)
        if (!acquired) {
          await this.releaseForShutdown(job)
          return
        }
        try {
          await this.executeManagedChain(
            job,
            state.handler,
            DEFAULT_MAX_DRAINS,
            state.config.heartbeatIntervalMs,
          )
        } finally {
          this.releaseInlineSlot(state)
        }
      })
      return
    }
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

  /** Return whether this type and dedupe key has a pending or active job. */
  async hasOutstanding(type: string, dedupeKey: string): Promise<boolean> {
    return this.backend.hasOutstanding(type, dedupeKey)
  }

  // --- Durable-orchestration journal passthroughs (§3.5) --------------------
  // Thin delegations so an Orchestrator depends only on JobQueue and never
  // reaches around it to the backend. Each throws OrchestrationUnsupportedError
  // if the backend lacks the capability.

  /** Throw if the backend cannot host durable orchestrations. */
  assertJournalCapable(): void {
    // Inline execution has no durable handoff after enqueue returns, so it
    // cannot model orchestration crash recovery even though process callbacks
    // now share the managed lifecycle path.
    if (this.backend.executesInline) {
      throw new OrchestrationUnsupportedError(
        'executesInline',
        'backend executes jobs inline during enqueue, so it cannot host durable ' +
          'orchestrations — use DummyBackend for orchestration unit tests',
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
   * Start the stuck-job reaper with one immediate recovery, then periodic
   * non-overlapping sweeps.
   *
   * The queue's `visibilityTimeoutMs` is the single lease-window source of
   * truth. A startup sweep failure is reported but does not disable later
   * sweeps. The timer is unref'd and cleared by {@link stopReaper} or
   * {@link shutdown}.
   *
   * @param intervalMs sweep cadence after the immediate recovery. Default: 60000.
   */
  startReaper(
    intervalMs = DEFAULT_REAPER_INTERVAL_MS,
  ): Promise<StartReaperResult> {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      return Promise.reject(
        new Error(
          `startReaper intervalMs must be a positive finite number of milliseconds, got ${intervalMs}`,
        ),
      )
    }
    if (typeof this.backend.recoverStuckJobs !== 'function') {
      return Promise.reject(
        new Error('startReaper: backend does not implement recoverStuckJobs'),
      )
    }
    if (this.reaperStarting) return this.reaperStarting
    if (this.reaperActive) {
      return Promise.resolve({ status: 'already-running' })
    }

    this.reaperActive = true

    // Self-scheduling, not setInterval: each periodic sweep settles before the
    // next timer is armed, so a slow backend can never overlap its own sweep.
    const tick = async (): Promise<void> => {
      if (!this.reaperActive || this.isShuttingDown) return
      await this.runReaperSweep('periodic')
      if (!this.reaperActive || this.isShuttingDown) return
      this.armReaper(tick, intervalMs)
    }

    const starting = this.runReaperSweep('startup')
      .then<StartReaperResult>((recovered) => {
        // stopReaper()/shutdown() may have run while startup was in flight.
        if (this.reaperActive && !this.isShuttingDown) {
          this.armReaper(tick, intervalMs)
        }
        return { status: 'started', recovered }
      })
      .finally(() => {
        this.reaperStarting = null
      })

    this.reaperStarting = starting
    return starting
  }

  /** Run one recovery sweep and isolate backend failures from the reaper loop. */
  private async runReaperSweep(
    phase: 'startup' | 'periodic',
  ): Promise<number | null> {
    try {
      const handled = await this.backend.recoverStuckJobs!(
        this.visibilityTimeoutMs,
      )
      if (handled > 0) {
        this.log.warn({ handled }, 'reaper recovered stuck jobs')
        this.emit({
          kind: 'reaper-recovered',
          handled,
          saturated: handled >= REAPER_SATURATION_HINT,
        })
      }
      return handled
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      this.log.error(
        { err, phase },
        'reaper sweep failed; will retry next interval',
      )
      this.emit({ kind: 'reaper-error', phase, message })
      return null
    }
  }

  /** Arm the next reaper tick. Unref'd — the reaper never keeps a process alive. */
  private armReaper(tick: () => Promise<void>, intervalMs: number): void {
    const timer = setTimeout(() => {
      this.reaperTimer = null
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

  /**
   * Stop claims immediately, allow a grace period, then cancel and release any
   * remaining managed claims. Release writes get a separate fixed one-second
   * bound so a broken backend cannot hang process shutdown.
   */
  async shutdown(timeoutMs = 30000): Promise<void> {
    this.isShuttingDown = true
    this.stopReaper()

    if (this.unsubscribePush) {
      this.unsubscribePush()
      this.unsubscribePush = null
    }
    for (const state of this.processors.values()) {
      state.running = false
      this.cancelInlineSlotWaiters(state)
    }
    this.cancelPendingSleeps()

    const active = [...this.managedRuns]
    if (active.length > 0) {
      const graceMs =
        Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 0
      await this.waitWithin(
        Promise.all(active.map((run) => run.settled)).then(() => undefined),
        graceMs,
      )
    }

    const remaining = [...this.managedRuns]
    if (remaining.length > 0) {
      for (const run of remaining) {
        run.stopHeartbeat()
        if (!run.controller.signal.aborted) {
          run.controller.abort(new Error('job queue shutdown'))
        }
      }

      const releases = remaining.map(async (run) => {
        try {
          await this.releaseForShutdown(run.handle)
        } catch (err) {
          this.log.warn(
            { err, jobId: run.handle.id },
            'failed to release job during shutdown',
          )
        }
      })
      await this.waitWithin(
        Promise.allSettled(releases).then(() => undefined),
        SHUTDOWN_RELEASE_WAIT_MS,
      )
    }

    await this.backend.shutdown(timeoutMs)
  }

  private acquireInlineSlot(state: ProcessorState): Promise<boolean> {
    if (!state.running || this.isShuttingDown) return Promise.resolve(false)
    if (state.activeCount < state.config.concurrency) {
      state.activeCount++
      return Promise.resolve(true)
    }
    return new Promise((resolve) => {
      state.inlineSlotWaiters.push(resolve)
    })
  }

  private releaseInlineSlot(state: ProcessorState): void {
    state.activeCount--
    while (state.inlineSlotWaiters.length > 0) {
      const waiter = state.inlineSlotWaiters.shift()
      if (!waiter) return
      if (!state.running || this.isShuttingDown) {
        waiter(false)
        continue
      }
      state.activeCount++
      waiter(true)
      return
    }
  }

  private cancelInlineSlotWaiters(state: ProcessorState): void {
    for (const waiter of state.inlineSlotWaiters.splice(0)) waiter(false)
  }

  private async releaseForShutdown(handle: JobHandle<unknown>): Promise<void> {
    const result = await handle.release()
    if (result.status === 'superseded') {
      this.emit({
        kind: 'superseded',
        type: handle.type,
        jobId: handle.id,
        durationMs: Math.max(
          0,
          Date.now() - (handle.claimedAt?.getTime() ?? Date.now()),
        ),
        reason: 'queued follow-up already covers this work',
      })
      return
    }
    if (result.status !== 'released') {
      this.log.warn(
        { jobId: handle.id, result },
        'job release did not apply during shutdown',
      )
      return
    }
    this.emit({
      kind: 'shutdown-released',
      type: handle.type,
      jobId: handle.id,
    })
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

  /** Run one processor claim through the shared managed execution chain. */
  private async processJob(state: ProcessorState, job: Job): Promise<void> {
    try {
      await this.executeManagedChain(
        this.createHandle(job),
        state.handler,
        DEFAULT_MAX_DRAINS,
        state.config.heartbeatIntervalMs,
      )
    } catch (err) {
      this.log.error({ err, jobId: job.id, type: job.type }, 'job handler failed')
    } finally {
      state.activeCount--
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
      'lease lost; skipping lifecycle write — job owned by another worker',
    )
    if (type) this.emit({ kind: 'lease-lost', type, jobId, op })
  }

  /** Bind a raw processor claim to the backend's fenced lifecycle methods. */
  private createHandle<T>(job: Job<T>): JobHandle<T> {
    const claimToken = job.claimToken
    return {
      ...job,
      status: 'active',
      complete: () => this.backend.complete(job.id, claimToken),
      fail: (reason: string) =>
        this.backend.fail(job.id, reason, claimToken),
      failFatal: (reason: string) =>
        this.backend.failFatal(job.id, reason, claimToken),
      heartbeat: () => this.backend.heartbeat(job.id, claimToken),
      release: () => this.backend.release(job.id, claimToken),
      log: (message: string) => {
        this.backend.log(job.id, message).catch((err: unknown) => {
          this.log.warn({ err, jobId: job.id }, 'failed to write job log entry')
        })
      },
    }
  }

  /** Strip every lifecycle method before exposing a claim to a handler. */
  private jobView<T>(handle: JobHandle<T>): Job<T> {
    return {
      id: handle.id,
      type: handle.type,
      data: handle.data,
      status: handle.status,
      attempt: handle.attempt,
      maxAttempts: handle.maxAttempts,
      priority: handle.priority,
      dedupeKey: handle.dedupeKey,
      dedupeScope: handle.dedupeScope,
      runAt: handle.runAt,
      createdAt: handle.createdAt,
      claimedAt: handle.claimedAt,
      completedAt: handle.completedAt,
      failedAt: handle.failedAt,
      failReason: handle.failReason,
      claimToken: handle.claimToken,
    }
  }

  private runAdmissionOpen(): boolean {
    return !this.isShuttingDown
  }

  private shutdownAdmissionError(cause?: unknown): Error {
    return new Error('JobQueue is shutting down; no new runs are accepted', {
      cause,
    })
  }

  private assertRunAdmission(): void {
    if (!this.runAdmissionOpen()) throw this.shutdownAdmissionError()
  }

  private async rejectClaimAfterShutdown<T>(
    handle: JobHandle<T>,
  ): Promise<never> {
    try {
      await this.releaseForShutdown(handle)
    } catch (err) {
      this.log.warn(
        { err, jobId: handle.id },
        'failed to release claim rejected during shutdown',
      )
      throw this.shutdownAdmissionError(err)
    }
    throw this.shutdownAdmissionError()
  }

  private assertValidMaxDrains(maxDrains: number): void {
    if (
      !Number.isFinite(maxDrains) ||
      maxDrains < 0 ||
      !Number.isInteger(maxDrains)
    ) {
      throw new Error(
        `maxDrains must be a non-negative finite integer, got ${maxDrains}`,
      )
    }
  }

  private async executeManagedChain<T>(
    initial: JobHandle<T>,
    handler: JobHandler<T>,
    maxDrains: number,
    heartbeatIntervalMs?: number,
  ): Promise<void> {
    let current: JobHandle<T> | null = initial
    let drains = 0
    let preservedError: { value: unknown } | null = null

    while (current) {
      let result: ManagedExecutionResult
      try {
        result = await this.executeManaged(
          current,
          handler,
          heartbeatIntervalMs,
        )
      } catch (err) {
        if (preservedError) throw preservedError.value
        throw err
      }

      if (result.status === 'superseded' && !preservedError) {
        preservedError = { value: result.handlerError }
      }
      if (
        result.status === 'stopped' ||
        !current.dedupeKey ||
        drains >= maxDrains ||
        !this.runAdmissionOpen()
      ) {
        break
      }

      let follower: JobHandle<T> | null
      try {
        follower = await this.backend.claimNextByKey<T>(
          current.type,
          current.dedupeKey,
        )
      } catch (err) {
        if (preservedError) throw preservedError.value
        throw err
      }
      if (!follower) break
      current = follower
      drains++
    }

    if (preservedError) throw preservedError.value
  }

  private beginManagedRun<T>(handle: JobHandle<T>): ManagedRun | null {
    if (!this.runAdmissionOpen()) return null

    const controller = new AbortController()
    let resolveSettled!: () => void
    const settled = new Promise<void>((resolve) => {
      resolveSettled = resolve
    })
    const run: ManagedRun = {
      handle: handle as JobHandle<unknown>,
      controller,
      stopHeartbeat: () => undefined,
      settled,
      resolveSettled,
    }
    this.managedRuns.add(run)
    return run
  }

  private async executeManaged<T>(
    handle: JobHandle<T>,
    handler: JobHandler<T>,
    heartbeatIntervalMs?: number,
  ): Promise<ManagedExecutionResult> {
    const run = this.beginManagedRun(handle)
    if (!run) return this.rejectClaimAfterShutdown(handle)

    run.stopHeartbeat = this.startManagedHeartbeat(run, heartbeatIntervalMs)

    const startedAt = Date.now()
    this.emit({
      kind: 'claimed',
      type: handle.type,
      jobId: handle.id,
      attempt: handle.attempt,
    })
    const context: JobContext = {
      signal: run.controller.signal,
      log: (message) => handle.log(message),
    }

    try {
      try {
        await handler(this.jobView(handle), context)
      } catch (handlerError) {
        run.stopHeartbeat()
        if (run.controller.signal.aborted) throw handlerError

        const reason =
          handlerError instanceof Error
            ? handlerError.message
            : String(handlerError)
        const fatal = handlerError instanceof FatalJobError
        try {
          if (fatal) {
            const result = await handle.failFatal(reason)
            if (result.status === 'lease-lost') {
              this.loseLease(run, 'failFatal')
            } else if (result.status === 'failed-terminal') {
              this.emit({
                kind: 'fail-fatal',
                type: handle.type,
                jobId: handle.id,
                reason,
              })
            }
          } else {
            const result = await handle.fail(reason)
            if (result.status === 'lease-lost') {
              this.loseLease(run, 'fail')
            } else if (result.status === 'superseded') {
              this.emit({
                kind: 'superseded',
                type: handle.type,
                jobId: handle.id,
                durationMs: Date.now() - startedAt,
                reason,
              })
              return { status: 'superseded', handlerError }
            } else if (
              result.status === 'retry-scheduled' ||
              result.status === 'failed-terminal'
            ) {
              this.emit({
                kind: 'failed',
                type: handle.type,
                jobId: handle.id,
                durationMs: Date.now() - startedAt,
                attempt: handle.attempt,
                maxAttempts: handle.maxAttempts,
                terminal: result.status === 'failed-terminal',
                reason,
              })
            }
          }
        } catch (recordingError) {
          throw new AggregateError(
            [handlerError, recordingError],
            'handler and failure recording both failed',
          )
        }
        throw handlerError
      }

      run.stopHeartbeat()
      if (run.controller.signal.aborted) return { status: 'stopped' }
      const result = await handle.complete()
      if (result.status === 'lease-lost') {
        this.loseLease(run, 'complete')
        return { status: 'stopped' }
      }
      if (result.status !== 'completed') return { status: 'stopped' }
      this.emit({
        kind: 'completed',
        type: handle.type,
        jobId: handle.id,
        durationMs: Date.now() - startedAt,
      })
      return { status: 'completed' }
    } finally {
      run.stopHeartbeat()
      this.managedRuns.delete(run)
      run.resolveSettled()
    }
  }

  private loseLease(run: ManagedRun, operation: string): void {
    run.stopHeartbeat()
    if (!run.controller.signal.aborted) {
      run.controller.abort(new LeaseLostError(operation))
    }
    this.warnLeaseLost(run.handle.id, operation, run.handle.type)
  }

  /**
   * Self-schedule heartbeats and enforce the local lease deadline independently.
   * A failed or hung renewal never lets the handler run past the last confirmed
   * lease window.
   */
  private startManagedHeartbeat(
    run: ManagedRun,
    intervalOverride?: number,
  ): () => void {
    const intervalMs =
      intervalOverride ??
      Math.max(1, Math.floor(this.visibilityTimeoutMs / 3))
    let heartbeatTimer: ReturnType<typeof setTimeout> | null = null
    let deadlineTimer: ReturnType<typeof setTimeout> | null = null
    let deadlineAt =
      (run.handle.claimedAt?.getTime() ?? Date.now()) +
      this.visibilityTimeoutMs
    let stopped = false

    const stop = (): void => {
      if (stopped) return
      stopped = true
      if (heartbeatTimer) clearTimeout(heartbeatTimer)
      if (deadlineTimer) clearTimeout(deadlineTimer)
      heartbeatTimer = null
      deadlineTimer = null
      run.controller.signal.removeEventListener('abort', stop)
    }
    const armDeadline = (): void => {
      if (stopped || run.controller.signal.aborted) return
      if (deadlineTimer) clearTimeout(deadlineTimer)
      deadlineTimer = setTimeout(
        () => this.loseLease(run, 'heartbeat-deadline'),
        Math.max(0, deadlineAt - Date.now()),
      )
      deadlineTimer.unref?.()
    }
    const scheduleHeartbeat = (): void => {
      if (stopped || run.controller.signal.aborted) return
      heartbeatTimer = setTimeout(() => {
        heartbeatTimer = null
        void beat()
      }, intervalMs)
      heartbeatTimer.unref?.()
    }
    const beat = async (): Promise<void> => {
      if (stopped || run.controller.signal.aborted) return
      try {
        const result = await run.handle.heartbeat()
        if (stopped || run.controller.signal.aborted) return
        if (result === 'lease-lost') {
          this.loseLease(run, 'heartbeat')
          return
        }
        deadlineAt = Date.now() + this.visibilityTimeoutMs
        armDeadline()
      } catch (err) {
        this.log.warn(
          { err, jobId: run.handle.id },
          'failed to heartbeat active job',
        )
      }
      scheduleHeartbeat()
    }

    run.controller.signal.addEventListener('abort', stop, { once: true })
    armDeadline()
    scheduleHeartbeat()
    return stop
  }

  /** Await work for at most ms and always clear the bound timer. */
  private async waitWithin(work: Promise<void>, ms: number): Promise<void> {
    if (ms <= 0) return
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, ms)
      timer.unref?.()
    })
    try {
      await Promise.race([work, timeout])
    } finally {
      clearTimeout(timer)
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
let globalQueueOptions: GlobalQueueOptions = {}
let globalQueueScopeActive = false

function configuredQueue(
  backend: IJobQueueBackend,
  options: GlobalQueueOptions,
): JobQueue {
  const { logger = defaultLogger, ...queueOptions } = options
  return new JobQueue(backend, logger, queueOptions)
}

/** Set the global backend and options (call in startup or test setup). */
export function setGlobalBackend(
  backend: IJobQueueBackend,
  options: GlobalQueueOptions = {},
): void {
  const queue = configuredQueue(backend, options)
  globalBackend = backend
  globalQueueOptions = options
  defaultQueue = queue
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

/** Create a JobQueue from the captured global config and explicit overrides. */
export function createJobQueue(
  backend?: IJobQueueBackend,
  overrides: GlobalQueueOptions = {},
): JobQueue {
  const selectedBackend = backend ?? globalBackend
  if (!selectedBackend) {
    throw new Error('JobQueue backend not set. Call setGlobalBackend() first.')
  }
  return configuredQueue(selectedBackend, {
    ...globalQueueOptions,
    ...overrides,
  })
}

/**
 * Run a callback with a temporary global queue, then restore the exact prior
 * global state and shut down only the temporary queue.
 */
export async function withGlobalQueue<T>(
  backend: IJobQueueBackend,
  ...scope:
    | [callback: () => T | Promise<T>]
    | [options: GlobalQueueOptions, callback: () => T | Promise<T>]
): Promise<T> {
  const options = scope.length === 1 ? {} : scope[0]
  const callback = scope.length === 1 ? scope[0] : scope[1]
  if (globalQueueScopeActive) {
    throw new Error('A global JobQueue scope is already active.')
  }

  globalQueueScopeActive = true
  const priorBackend = globalBackend
  const priorQueue = defaultQueue
  const priorOptions = globalQueueOptions
  let temporaryQueue: JobQueue | null = null

  try {
    temporaryQueue = configuredQueue(backend, options)
    globalBackend = backend
    globalQueueOptions = options
    defaultQueue = temporaryQueue
    return await callback()
  } finally {
    globalBackend = priorBackend
    globalQueueOptions = priorOptions
    defaultQueue = priorQueue
    try {
      if (temporaryQueue) await temporaryQueue.shutdown()
    } finally {
      globalQueueScopeActive = false
    }
  }
}
