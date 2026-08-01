/**
 * `durabl/testing` — unit-test harness for orchestrator bodies.
 *
 * Runs an {@link OrchestratorFn} against the real step machine
 * ({@link buildContext}) over an in-memory journal: no queue, no backend, no
 * job-type registration, no timers. The serialization guard runs on every
 * append, so a non-BSON-serializable step result fails the test the same way
 * it would fail in production.
 *
 * ```ts
 * const t = testOrchestration(restartTrial, { data: { userId } })
 * await t.crashAfter('create-sub')   // run aborts after the step commits
 * await t.resume()                   // fresh attempt, same journal
 * expect(stripe.createSubscription).toHaveBeenCalledTimes(1)
 * ```
 *
 * `now()`/`uuid()` are deterministic by default (fixed bootstrap seed), so
 * step results are snapshot-stable across runs and machines.
 */
import {
  DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
  fromStored,
  guardAppend,
  sortBySeq,
  toStored,
} from '../journal/serialize'
import {
  BOOTSTRAP_NAME,
  BOOTSTRAP_SEQ,
  buildContext,
  type StepJournalPort,
} from '../orchestrator/context'
import type { OrchestratorFn } from '../orchestrator/types'
import type { AppendStepResult, Job, StepRecord } from '../types'

/** Default `now()` for deterministic bootstraps: 2025-01-01T00:00:00Z. */
export const TEST_STARTED_AT = 1_735_689_600_000

/** Default `uuid(label)` seed for deterministic bootstraps. */
export const TEST_SEED = 'durabl-test-seed'

/**
 * Thrown by the harness's journal port to simulate a worker crash at a step
 * boundary. Swallowed by {@link TestOrchestration.crashBefore} /
 * {@link TestOrchestration.crashAfter}; exported so custom assertions can
 * catch it explicitly.
 */
export class SimulatedCrash extends Error {
  constructor(step: string, phase: 'before' | 'after') {
    super(`simulated crash ${phase} step '${step}'`)
    this.name = 'SimulatedCrash'
  }
}

export interface TestOrchestrationOptions<T> {
  /** Job payload handed to the body as `job.data`. */
  data?: T
  jobId?: string
  /** Job type (cosmetic — no registration happens). */
  type?: string
  /**
   * Per-step timeout. Default `0` = no timeout, so tests never leave a stray
   * timer; pass a value to exercise `StepTimeout` behavior.
   */
  stepTimeoutMs?: number
  /** Frozen `octx.now()` value. Default {@link TEST_STARTED_AT}. */
  now?: number
  /** `octx.uuid()` derivation seed. Default {@link TEST_SEED}. */
  seed?: string
}

/** A journaled user step, result already decoded (void sentinel → undefined). */
export interface RecordedStep {
  seq: number
  name: string
  result: unknown
}

interface CrashPoint {
  phase: 'before' | 'after'
  step: string
}

/** Harness returned by {@link testOrchestration}. */
export class TestOrchestration<T> {
  /** The job every attempt sees (same `id` — resume is the same job). */
  readonly job: Job<T>
  /** Lines written via `octx.log`, across all attempts. */
  readonly logs: string[] = []
  /** Completed attempts (each `run`/`resume`/`crash*` call is one). */
  attempts = 0

  private readonly fn: OrchestratorFn<T>
  private readonly stepTimeoutMs: number
  private readonly journal: StepRecord[] = []
  private journalBytes = 0

  constructor(fn: OrchestratorFn<T>, opts: TestOrchestrationOptions<T> = {}) {
    this.fn = fn
    this.stepTimeoutMs = opts.stepTimeoutMs ?? 0
    const createdAt = new Date(opts.now ?? TEST_STARTED_AT)
    this.job = {
      id: opts.jobId ?? 'test-job-1',
      type: opts.type ?? 'test-orchestration',
      data: opts.data as T,
      status: 'active',
      attempt: 0,
      maxAttempts: 25,
      priority: 0,
      runAt: createdAt,
      createdAt,
      claimToken: 'test-claim-token',
    }
    // Pre-seed the bootstrap record so now()/uuid() are deterministic. A body
    // that never calls them never reads it — same cost as production.
    this.journal.push({
      seq: BOOTSTRAP_SEQ,
      name: BOOTSTRAP_NAME,
      result: toStored({
        startedAt: opts.now ?? TEST_STARTED_AT,
        seed: opts.seed ?? TEST_SEED,
      }),
      ts: createdAt,
    })
  }

  /** Journaled user steps (bootstrap excluded), ascending seq, decoded. */
  get steps(): RecordedStep[] {
    return sortBySeq(this.journal)
      .filter((s) => s.seq !== BOOTSTRAP_SEQ)
      .map((s) => ({ seq: s.seq, name: s.name, result: fromStored(s.result) }))
  }

  /**
   * Run one full attempt. Body errors propagate — including
   * `NondeterminismError` when a resumed body no longer matches the journal.
   */
  async run(): Promise<void> {
    await this.attempt(undefined)
  }

  /** Alias of {@link run} that reads as intent: a post-crash re-attempt. */
  async resume(): Promise<void> {
    await this.attempt(undefined)
  }

  /**
   * Resume with a DIFFERENT body against the journal built so far — the
   * deploy-while-in-flight scenario. Asserting this rejects with
   * `NondeterminismError` (or completes cleanly) is how a body edit proves it
   * is replay-compatible before it ships.
   */
  async resumeWith(fn: OrchestratorFn<T>): Promise<void> {
    await this.attempt(undefined, fn)
  }

  /**
   * Run an attempt that crashes BEFORE `step`'s result is journaled: the
   * step's side effect fired but the journal missed it, so a later
   * {@link resume} re-runs the step. This is the driver-retry/crash window
   * that per-step idempotency keys exist for.
   */
  async crashBefore(step: string): Promise<void> {
    await this.attempt({ phase: 'before', step })
  }

  /**
   * Run an attempt that crashes AFTER `step` commits to the journal: a later
   * {@link resume} skips it and replays the stored result.
   */
  async crashAfter(step: string): Promise<void> {
    await this.attempt({ phase: 'after', step })
  }

  private async attempt(
    crash: CrashPoint | undefined,
    fn: OrchestratorFn<T> = this.fn,
  ): Promise<void> {
    this.attempts += 1
    this.job.attempt = this.attempts
    const octx = buildContext({
      journal: this.port(crash),
      job: this.job,
      claimToken: this.job.claimToken as string,
      log: (message) => this.logs.push(message),
      steps: [...this.journal],
      stepTimeoutMs: this.stepTimeoutMs,
      runController: new AbortController(),
    })
    try {
      await fn(this.job, octx)
    } catch (err) {
      if (crash && err instanceof SimulatedCrash) return
      throw err
    }
    if (crash) {
      throw new Error(
        `expected a crash at step '${crash.step}' but the body completed — ` +
          'is the step name spelled correctly?',
      )
    }
  }

  private port(crash: CrashPoint | undefined): StepJournalPort {
    return {
      appendStep: async (
        _jobId,
        _claimToken,
        record,
      ): Promise<AppendStepResult> => {
        if (crash?.phase === 'before' && record.name === crash.step) {
          throw new SimulatedCrash(crash.step, 'before')
        }
        const existing = this.journal.find((s) => s.seq === record.seq)
        if (existing) return { status: 'already-recorded', existing }
        // Same guard as production backends: non-serializable results and
        // oversized journals fail here, in the unit test.
        this.journalBytes += guardAppend(
          record,
          this.journalBytes,
          DEFAULT_JOURNAL_SOFT_LIMIT_BYTES,
        )
        this.journal.push(record)
        if (crash?.phase === 'after' && record.name === crash.step) {
          throw new SimulatedCrash(crash.step, 'after')
        }
        return { status: 'appended' }
      },
      heartbeatClaimed: async () => 'heartbeated',
    }
  }
}

/**
 * Create a {@link TestOrchestration} for an orchestrator body. See module doc
 * for the crash/resume assertion pattern.
 */
export function testOrchestration<T>(
  fn: OrchestratorFn<T>,
  opts: TestOrchestrationOptions<T> = {},
): TestOrchestration<T> {
  return new TestOrchestration(fn, opts)
}
