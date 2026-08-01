/**
 * Per-run {@link OrchestratorContext} construction and the lease/timeout
 * machinery that wraps a user orchestrator body.
 */
import { createHash, randomBytes } from 'node:crypto'

import { NondeterminismError, StepTimeout } from '../journal/errors'
import { fromStored, toStored } from '../journal/serialize'
import type {
  AppendStepResult,
  HeartbeatClaimedResult,
  Job,
  StepRecord,
} from '../types'

import type {
  OrchestratorContext,
  StepKeys,
  StepOptions,
} from './types'

/**
 * Reserved seq for the journaled bootstrap record (§4.1/D1). Lives OUTSIDE the
 * user-step seq space (user seqs start at 0) so it can never collide with a
 * `step()` call and never trips divergence detection.
 */
export const BOOTSTRAP_SEQ = -1

/** Reserved name for the journaled bootstrap record. §4.1/D1. */
export const BOOTSTRAP_NAME = '$bootstrap'

/**
 * The one journaled bootstrap record backing `now()`/`uuid(label)` (§4.1/D1):
 * captured lazily on the run's first use, read back on every resume so both
 * helpers are frozen for the life of the job.
 */
export interface BootstrapValues {
  /** Wall clock (ms) at the first `now()`/`uuid()` use of the first attempt. */
  startedAt: number
  /** Crypto-random hex; namespaces `uuid(label)` derivation per job. */
  seed: string
}

/**
 * Deterministic UUID from the bootstrap `seed` + `label`. Stable across calls
 * and resumes (the seed is journaled once), distinct per label and per job.
 * NOT cryptographically random — predictable given the seed (do not use for
 * secrets). §4.1.
 */
export function deriveUuid(seed: string, label: string): string {
  const h = createHash('sha256')
    .update(seed)
    .update('\0')
    .update(label)
    .digest('hex')
  // Format the first 32 hex chars as a UUID (version/variant nibbles forced so
  // it reads as a valid v4-shaped string; randomness is NOT implied).
  return [
    h.slice(0, 8),
    h.slice(8, 12),
    `4${h.slice(13, 16)}`,
    `${((parseInt(h[16], 16) & 0x3) | 0x8).toString(16)}${h.slice(17, 20)}`,
    h.slice(20, 32),
  ].join('-')
}

/**
 * The narrow seam the step machine writes through. `JobQueue` satisfies it
 * structurally; tests (and `durabl/testing`) supply an in-memory
 * implementation, so replay/bootstrap/duplicate-append behavior is exercised
 * with no queue, backend, or job-type registration involved.
 */
export interface StepJournalPort {
  appendStep(
    jobId: string,
    claimToken: string,
    record: StepRecord,
  ): Promise<AppendStepResult>
  heartbeatClaimed(
    jobId: string,
    claimToken: string,
  ): Promise<HeartbeatClaimedResult>
}

interface BuildContextArgs<T> {
  journal: StepJournalPort
  job: Job<T>
  claimToken: string
  /** Sink for `octx.log` lines (the queue's fire-and-forget `ctx.log`). */
  log: (message: string) => void
  steps: StepRecord[]
  stepTimeoutMs: number
  /**
   * Run-level abort controller. Aborted (with the causing error as reason)
   * when the lease is lost or `maxDurationMs` fires; `step()` refuses to start
   * once it is aborted so an orphaned body stops at the next step boundary.
   */
  runController: AbortController
}

/** Sentinel thrown internally when the lease is lost; mapped to a retry. */
export class LeaseLostError extends Error {
  constructor() {
    super('orchestrator lease lost')
    this.name = 'LeaseLostError'
  }
}

/**
 * Build the durable context for one run. `seq` is assigned synchronously at each
 * `step()` call so order is deterministic even under concurrent fan-out (§3.3).
 */
export function buildContext<T>(args: BuildContextArgs<T>): OrchestratorContext {
  const { journal, job, claimToken, log, steps, stepTimeoutMs, runController } =
    args
  const journalBySeq = new Map<number, StepRecord>(
    steps.filter((s) => s.seq !== BOOTSTRAP_SEQ).map((s) => [s.seq, s]),
  )
  let nextSeq = 0

  // --- Bootstrap record (§4.1/D1) -----------------------------------------
  // Journaled once on the run's first now()/uuid() use; read back on resume so
  // now() is frozen at the FIRST attempt's start and uuid(label) derives from
  // one stable seed. Runs that never call now()/uuid() pay nothing.
  const recordedBootstrap = steps.find((s) => s.seq === BOOTSTRAP_SEQ)
  let bootstrap: BootstrapValues | undefined = recordedBootstrap
    ? (fromStored(recordedBootstrap.result) as BootstrapValues)
    : undefined
  /**
   * Pending durable write of a freshly minted bootstrap. `step()` awaits it
   * before its own append so no user step is journaled ahead of the bootstrap
   * it may depend on — otherwise a crash in between would resume with a
   * DIFFERENT `now()`/seed than the journaled steps observed.
   */
  let bootstrapAppend: Promise<void> | undefined

  const ensureBootstrap = (): BootstrapValues => {
    if (bootstrap) return bootstrap
    bootstrap = {
      startedAt: Date.now(),
      seed: randomBytes(16).toString('hex'),
    }
    const record: StepRecord = {
      seq: BOOTSTRAP_SEQ,
      name: BOOTSTRAP_NAME,
      result: toStored(bootstrap),
      ts: new Date(),
    }
    bootstrapAppend = journal
      .appendStep(job.id, claimToken, record)
      .then((outcome) => {
        if (outcome.status === 'lease-lost') throw new LeaseLostError()
        // 'already-recorded' can only be a driver-retry of OUR write (same
        // token, same run) — the stored values are the ones we minted.
      })
    // Swallow the background rejection so a body that never steps again does
    // not surface an unhandled rejection; step() re-awaits the original
    // promise and gets the real error.
    bootstrapAppend.catch(() => {})
    return bootstrap
  }

  const step = async <R>(
    name: string,
    fn: (keys: StepKeys, signal: AbortSignal) => Promise<R>,
    opts?: StepOptions,
  ): Promise<R> => {
    // Run aborted (lease lost / maxDurationMs fired): stop at the step boundary
    // BEFORE firing any side effect. The reason is LeaseLostError or
    // MaxDurationExceeded, set by whoever aborted. §7.
    if (runController.signal.aborted) {
      throw runController.signal.reason instanceof Error
        ? runController.signal.reason
        : new LeaseLostError()
    }

    const seq = nextSeq++ // synchronous: fixes order before any await (§3.3)

    const recorded = journalBySeq.get(seq)
    if (recorded) {
      if (recorded.name !== name) {
        // Divergence detected on resume → fatal (§6).
        throw new NondeterminismError(job.id, seq, recorded.name, name)
      }
      return fromStored(recorded.result) as R
    }

    const idempotencyKey =
      opts?.idempotencyKey ?? `${job.id}:${seq}:${name}`
    const keys: StepKeys = { idempotencyKey, jobId: job.id, seq }
    const timeoutMs = opts?.timeoutMs ?? stepTimeoutMs

    const result = await runWithTimeout(
      (signal) => fn(keys, signal),
      timeoutMs,
      name,
      runController.signal,
    )

    const record: StepRecord = {
      seq,
      name,
      result: toStored(result),
      ts: new Date(),
    }
    // Durable-write ordering: a freshly minted bootstrap must land before any
    // user step that could depend on it (see `bootstrapAppend` above).
    if (bootstrapAppend) await bootstrapAppend
    const outcome = await journal.appendStep(job.id, claimToken, record)
    if (outcome.status === 'lease-lost') throw new LeaseLostError()
    if (outcome.status === 'already-recorded') {
      // Driver-retry ambiguity after a successful write: return the stored
      // value (carried on the result — no journal re-read) rather than
      // appending a duplicate (§3.6).
      return fromStored(outcome.existing.result) as R
    }
    journalBySeq.set(seq, record)
    return result
  }

  return {
    step,
    now: () => ensureBootstrap().startedAt,
    uuid: (label: string) => deriveUuid(ensureBootstrap().seed, label),
    log: (message: string) => log(message),
    heartbeat: async () => {
      const res = await journal.heartbeatClaimed(job.id, claimToken)
      if (res === 'lease-lost' && !runController.signal.aborted) {
        // Surface it: abort the run so the next step boundary stops the body.
        runController.abort(new LeaseLostError())
      }
      return res
    },
    signal: runController.signal,
  }
}

/**
 * Run `fn` with a cooperative timeout. On expiry, abort the signal and reject
 * with {@link StepTimeout} (retryable). The fn must thread `signal` into
 * cancellable APIs for the abort to actually stop work. When `runSignal` is
 * given, the per-step signal is chained to it — a run-level abort (lease lost,
 * maxDurationMs) also aborts the in-flight step. §7.3.
 */
export function runWithTimeout<R>(
  fn: (signal: AbortSignal) => Promise<R>,
  timeoutMs: number,
  name: string,
  runSignal?: AbortSignal,
): Promise<R> {
  const controller = new AbortController()
  const onRunAbort = (): void => controller.abort(runSignal?.reason)
  if (runSignal) {
    if (runSignal.aborted) onRunAbort()
    else runSignal.addEventListener('abort', onRunAbort, { once: true })
  }
  const unchain = (): void =>
    runSignal?.removeEventListener('abort', onRunAbort)

  if (!timeoutMs || timeoutMs <= 0) {
    return fn(controller.signal).finally(unchain)
  }

  return new Promise<R>((resolve, reject) => {
    const timer = setTimeout(() => {
      controller.abort()
      unchain()
      reject(new StepTimeout(name, timeoutMs))
    }, timeoutMs)
    fn(controller.signal).then(
      (r) => {
        clearTimeout(timer)
        unchain()
        resolve(r)
      },
      (e) => {
        clearTimeout(timer)
        unchain()
        reject(e)
      },
    )
  })
}

/** Result of {@link startHeartbeat}. */
export interface HeartbeatHandle {
  stop(): void
}

/**
 * Self-scheduling, awaited, error-caught lease renewer. Never overlaps (the next
 * tick is scheduled only after the previous write settles), never an unhandled
 * rejection, re-armed until stopped. A `'lease-lost'` result means another
 * worker owns the job: the loop stops for good (renewing a foreign lease is
 * pointless) and `onLeaseLost` fires exactly once so the caller can abort the
 * orphaned run. Transient errors (thrown) warn and re-arm. §7.2.
 */
export function startHeartbeat(
  journal: StepJournalPort,
  jobId: string,
  claimToken: string,
  intervalMs: number,
  onWarn: (err: unknown) => void,
  onLeaseLost: () => void,
): HeartbeatHandle {
  let stopped = false
  let timer: ReturnType<typeof setTimeout>

  const tick = async (): Promise<void> => {
    if (stopped) return
    try {
      const res = await journal.heartbeatClaimed(jobId, claimToken)
      // stop() may have fired while this write was in flight (the run completed
      // or failed and set the status): honor it so a post-stop 'lease-lost'
      // read is ignored instead of aborting an already-finished run.
      if (stopped) return
      if (res === 'lease-lost') {
        stopped = true
        onLeaseLost()
        return
      }
    } catch (err) {
      if (stopped) return
      onWarn(err)
    }
    if (!stopped) timer = setTimeout(() => void tick(), intervalMs)
  }

  timer = setTimeout(() => void tick(), intervalMs)
  return {
    stop() {
      stopped = true
      clearTimeout(timer)
    },
  }
}
