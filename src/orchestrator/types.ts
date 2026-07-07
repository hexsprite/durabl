/**
 * Public types for the durable-execution {@link Orchestrator} layer.
 */
import type { HeartbeatClaimedResult, Job, ProcessorConfig } from '../types'

/** Keys passed INTO a step fn for idempotent external calls. §4. */
export interface StepKeys {
  /**
   * Default idempotency key, stable across resumes of THIS job:
   * `${jobId}:${seq}:${name}`. Defends "don't double-fire across retries of
   * THIS job." Does NOT defend against a DIFFERENT job for the same entity —
   * for that, pass `opts.idempotencyKey` (entity-scoped) or `dedupeKey` on
   * enqueue. §9.
   */
  idempotencyKey: string
  jobId: string
  seq: number
}

/** Per-step options. §4. */
export interface StepOptions {
  /** Override the default jobId-scoped idempotency key (e.g. entity-scoped). */
  idempotencyKey?: string
  /** Override the orchestration-wide `stepTimeoutMs` for this step. §7.3. */
  timeoutMs?: number
}

/**
 * Context handed to an orchestrator body as the 2nd argument. The determinism
 * rule: the only things a control path may `await` are `octx.*` (i.e.
 * `octx.step`/`octx.heartbeat`) or `Promise.all`/`allSettled` over steps. §2.
 */
export interface OrchestratorContext {
  /**
   * Memoized, journaled step. Runs `fn` once; on resume returns the journaled
   * result without re-running. The idempotency key is passed INTO `fn`; override
   * at the call site via `opts.idempotencyKey`. §4/§5/§6.
   */
  step<R>(
    name: string,
    fn: (keys: StepKeys, signal: AbortSignal) => Promise<R>,
    opts?: StepOptions,
  ): Promise<R>

  /**
   * Frozen logical start time of this run: the wall clock at the first
   * `now()`/`uuid()` use of the FIRST attempt, captured in a single journaled
   * bootstrap record and read back on resume — identical on every call and on
   * every resume, synchronous after the first use. For a fresh wall-clock
   * reading, use an explicit `step('read-clock', () => Date.now())`. §4.1/D1.
   */
  now(): number

  /**
   * Deterministic UUID derived from the journaled bootstrap seed + `label`:
   * stable across calls and resumes, distinct per label and per job, journaled
   * once (the seed), not per call. **Predictable — NOT for secrets**; for a
   * crypto-random value stable across resume use a journaled
   * `step('token', () => randomUUID())`. §4.1/D1.
   */
  uuid(label: string): string

  /** Append a log line to the job. */
  log(message: string): void

  /**
   * Manual lease extension (escape hatch; auto-heartbeat already runs). Returns
   * `'lease-lost'` when another worker holds the job — in that case the run's
   * {@link signal} is aborted and the next `step()` throws, so the orphaned body
   * stops at the step boundary. §7.
   */
  heartbeat(): Promise<HeartbeatClaimedResult>

  /**
   * Run-level abort signal. Aborted when the lease is lost (job reclaimed by
   * another worker) or `maxDurationMs` fires; `signal.reason` is the
   * corresponding error. Thread it into long-running non-step work so an
   * orphaned body stops promptly — `step()` already refuses to start once it
   * is aborted. §7.
   */
  signal: AbortSignal
}

/** Per-type orchestrator configuration. §4. */
export interface OrchestratorConfig extends ProcessorConfig {
  /** Heartbeat cadence. Default: `visibilityTimeoutMs / 3`. §7.1. */
  heartbeatIntervalMs?: number
  /**
   * Lease window for this orchestrator. Defaults to the queue's reaper timeout;
   * an override LOOSER than the reaper timeout is a startup error. §7.1.
   */
  visibilityTimeoutMs?: number
  /** Per-step liveness cap. Default: `visibilityTimeoutMs`. §7.3. */
  stepTimeoutMs?: number
  /** Whole-orchestration cap (no default — unbounded unless set). §7.3. */
  maxDurationMs?: number
}

/** An orchestrator body: a job handler whose 2nd arg is the durable context. */
export type OrchestratorFn<T> = (
  job: Job<T>,
  octx: OrchestratorContext,
) => Promise<void>
