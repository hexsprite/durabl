/**
 * Orchestrator — DBOS-style step-level durable execution layered on
 * {@link JobQueue}. An orchestrator is just a job type whose handler is a
 * wrapper that journals completed steps and skips them on resume. The
 * load-bearing claim primitive is untouched. See `docs/orchestrator-spec.md`.
 */
import type { JobQueue } from '../JobQueue'
import {
  HeartbeatConfigConflict,
  isFatalOrchestrationError,
  MaxDurationExceeded,
  OrchestrationUnsupportedError,
  OrchestratorTypeConflict,
} from '../journal/errors'
import { defaultLogger, type Logger } from '../logger'
import type { Job, JobContext } from '../types'

import { buildContext, LeaseLostError, startHeartbeat } from './context'
import type { OrchestratorConfig, OrchestratorFn } from './types'

interface ResolvedConfig {
  concurrency: number
  pollInterval?: number
  heartbeatIntervalMs: number
  stepTimeoutMs: number
  maxDurationMs?: number
}

/**
 * Reject a non-positive/non-finite ms override at `define()` time. A zero
 * `visibilityTimeoutMs` would otherwise size the heartbeat to `setTimeout(0)`
 * (a write hammer) and silently disable step timeouts (`stepTimeoutMs: 0`).
 */
function assertPositiveFiniteMs(name: string, value: number | undefined): void {
  if (value === undefined) return
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(
      `OrchestratorConfig.${name} must be a positive finite number of milliseconds, got ${value}`,
    )
  }
}

/**
 * Derive the heartbeat cadence from the lease window: a third of it, but never
 * below 1ms. A 1-2ms `visibilityTimeoutMs` floors `visibilityTimeoutMs / 3` to
 * 0 (it passes the positive-finite guard), which would size the heartbeat to
 * `setTimeout(0)` — a per-tick write hammer. Clamp so the cadence is always a
 * real interval.
 */
export function deriveHeartbeatIntervalMs(visibilityTimeoutMs: number): number {
  return Math.max(1, Math.floor(visibilityTimeoutMs / 3))
}

export class Orchestrator {
  private queue: JobQueue
  private log: Logger

  /**
   * @param queue a {@link JobQueue} whose backend is journal-capable (asserted
   *   here; throws {@link OrchestrationUnsupportedError} otherwise).
   */
  constructor(queue: JobQueue, logger: Logger = defaultLogger) {
    queue.assertJournalCapable()
    this.queue = queue
    this.log = logger.child({ category: 'Orchestrator' })
  }

  /**
   * Register a durable orchestrator for `type`. Refuses a type already claimed
   * by a `process()` handler (or another `define()`), surfaced as
   * {@link OrchestratorTypeConflict}. §3.4.
   */
  define<T>(
    type: string,
    fn: OrchestratorFn<T>,
    config: OrchestratorConfig = {},
  ): void {
    const resolved = this.resolveConfig(config)
    const wrapper = this.makeWrapper(type, fn, resolved)

    try {
      this.queue.process(type, wrapper, {
        concurrency: resolved.concurrency,
        ...(resolved.pollInterval !== undefined
          ? { pollInterval: resolved.pollInterval }
          : {}),
      })
    } catch (err) {
      // JobQueue.process throws "already registered" for a duplicate type.
      if (err instanceof Error && err.message.includes('already registered')) {
        throw new OrchestratorTypeConflict(type)
      }
      throw err
    }
  }

  private resolveConfig(config: OrchestratorConfig): ResolvedConfig {
    assertPositiveFiniteMs('visibilityTimeoutMs', config.visibilityTimeoutMs)
    assertPositiveFiniteMs('heartbeatIntervalMs', config.heartbeatIntervalMs)
    assertPositiveFiniteMs('stepTimeoutMs', config.stepTimeoutMs)
    assertPositiveFiniteMs('maxDurationMs', config.maxDurationMs)
    assertPositiveFiniteMs('pollInterval', config.pollInterval)

    const reaperMs = this.queue.visibilityTimeoutMs
    const visibilityTimeoutMs = config.visibilityTimeoutMs ?? reaperMs
    // A looser override cannot actually hold the lease — reject at startup (R3).
    if (config.visibilityTimeoutMs !== undefined && visibilityTimeoutMs > reaperMs) {
      throw new HeartbeatConfigConflict(visibilityTimeoutMs, reaperMs)
    }
    return {
      concurrency: config.concurrency ?? 1,
      pollInterval: config.pollInterval,
      heartbeatIntervalMs:
        config.heartbeatIntervalMs ??
        deriveHeartbeatIntervalMs(visibilityTimeoutMs),
      stepTimeoutMs: config.stepTimeoutMs ?? visibilityTimeoutMs,
      maxDurationMs: config.maxDurationMs,
    }
  }

  /** The wrapper registered with `JobQueue.process()`. §3.4. */
  private makeWrapper<T>(
    type: string,
    fn: OrchestratorFn<T>,
    config: ResolvedConfig,
  ): (job: Job<T>, ctx: JobContext) => Promise<void> {
    return async (job, ctx) => {
      const claimToken = job.claimToken
      if (!claimToken) {
        // Backend must mint a claim token on claim for fencing to work. This
        // is a backend-capability defect, not a transient failure — every
        // retry would hit it again and burn every attempt, so fail fatally on
        // the first one. (ctx.failFatal is itself lease-fenced upstream, but
        // with no token here the write is unfenced by construction.)
        const err = new OrchestrationUnsupportedError('claimToken')
        this.log.error(
          { jobId: job.id, type },
          `orchestrator '${type}' claimed a job with no claimToken; backend must mint one`,
        )
        await ctx.failFatal(err.message)
        return
      }

      const steps = await this.queue.readSteps(job.id)
      // Run-level abort: fired on lease loss or maxDurationMs so the body (and
      // any in-flight step signal chained to it) stops instead of orphaning.
      const runController = new AbortController()
      const octx = buildContext({
        journal: this.queue,
        job,
        claimToken,
        log: (message) => ctx.log(message),
        steps,
        stepTimeoutMs: config.stepTimeoutMs,
        runController,
      })

      const heart = startHeartbeat(
        this.queue,
        job.id,
        claimToken,
        config.heartbeatIntervalMs,
        (err) =>
          this.log.warn(
            { err, jobId: job.id },
            'heartbeat failed; will retry next tick',
          ),
        () => {
          this.log.warn(
            { jobId: job.id },
            'lease lost — job reclaimed; aborting run',
          )
          if (!runController.signal.aborted) {
            runController.abort(new LeaseLostError())
          }
        },
      )

      try {
        await this.runBody(fn, job, octx, config.maxDurationMs, runController)

        // Stop the heartbeat BEFORE completing. Otherwise a tick already in
        // flight when the status flips to 'completed' reads 'lease-lost' and
        // fires a spurious 'lease lost — aborting run' warn + run abort on an
        // already-finished run. (startHeartbeat also re-checks its stopped flag
        // after the await, so a tick that resolves post-stop is a no-op.)
        heart.stop()

        const done = await this.queue.completeClaimed(job.id, claimToken)
        if (done === 'lease-lost') {
          // A different token owns this job now — it was genuinely reclaimed
          // by another worker (a reaper-failed job under OUR token would have
          // matched via the active|failed filter and completed). Yield — do
          // NOT throw: the reclaimed copy is the live run.
          this.log.warn(
            { jobId: job.id },
            'job reclaimed by another worker before completion; yielding',
          )
        }
      } catch (err) {
        if (err instanceof LeaseLostError) {
          this.log.warn(
            { jobId: job.id },
            'lease lost mid-step; yielding to recovered attempt',
          )
          return
        }
        if (isFatalOrchestrationError(err)) {
          const reason = err instanceof Error ? err.message : String(err)
          this.log.error(
            { err, jobId: job.id, type },
            'orchestration failed fatally',
          )
          // Stop before failFatal for the same reason as the completion path:
          // don't let an in-flight tick race the status write into a spurious
          // lease-loss.
          heart.stop()
          await ctx.failFatal(reason)
          return
        }
        // Normal throw → processJob.fail() → jittered backoff → resume.
        throw err
      } finally {
        heart.stop()
      }
    }
  }

  private async runBody<T>(
    fn: OrchestratorFn<T>,
    job: Job<T>,
    octx: Parameters<OrchestratorFn<T>>[1],
    maxDurationMs: number | undefined,
    runController: AbortController,
  ): Promise<void> {
    if (!maxDurationMs || maxDurationMs <= 0) {
      await fn(job, octx)
      return
    }
    let timer: ReturnType<typeof setTimeout> | undefined
    const cap = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const err = new MaxDurationExceeded(maxDurationMs)
        // Cancel the body too: the orphaned run must stop at its next step
        // boundary instead of racing on and firing side effects.
        if (!runController.signal.aborted) runController.abort(err)
        reject(err)
      }, maxDurationMs)
    })
    try {
      await Promise.race([fn(job, octx), cap])
    } finally {
      if (timer) clearTimeout(timer)
    }
  }
}
