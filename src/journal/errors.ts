/**
 * Durable-execution error sentinels.
 *
 * Shared by the journal-capable backends (which throw the guard errors at the
 * write boundary) and the {@link Orchestrator} wrapper (which maps the fatal
 * ones to `failFatal`). Lives in `journal/` so neither layer has to import the
 * other — both depend on this leaf module.
 */

/**
 * Throw inside an orchestrator step (or body) to fail the whole orchestration
 * terminally with **no retry**. Maps to `failFatal`.
 */
export class NonRetryable extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'NonRetryable'
  }
}

/**
 * A resumed orchestrator called `step()` at a sequence whose journaled record
 * has a different name — the control path diverged from the recorded run.
 * Fatal (retrying re-runs the same divergent code → same error). §6.
 */
export class NondeterminismError extends Error {
  constructor(
    public readonly jobId: string,
    public readonly seq: number,
    public readonly expected: string,
    public readonly actual: string,
  ) {
    super(
      `nondeterminism at seq ${seq} for job ${jobId}: journal has '${expected}', code called '${actual}'`,
    )
    this.name = 'NondeterminismError'
  }
}

/**
 * A step returned a value that is not BSON-serializable (class instance,
 * function, cycle, `undefined`-valued key). Caught before it corrupts the
 * journal. Fatal. §8.
 */
export class NonSerializableStepResult extends Error {
  constructor(
    public readonly step: string,
    reason: string,
  ) {
    super(`step '${step}' returned a non-serializable result: ${reason}`)
    this.name = 'NonSerializableStepResult'
  }
}

/**
 * The cumulative journal (`steps[]` + `logs[]`) would exceed the soft cap on
 * this append. Thrown before hitting the opaque 16MB BSON limit mid-flow.
 * Fatal. §8.1.
 */
export class JournalTooLarge extends Error {
  constructor(
    public readonly step: string,
    public readonly approxBytes: number,
  ) {
    super(
      `journal too large appending step '${step}': ~${approxBytes} bytes exceeds soft cap`,
    )
    this.name = 'JournalTooLarge'
  }
}

/**
 * A step did not settle within its timeout. Retryable (a transient hang retries
 * from resume rather than terminating the flow). §7.3.
 */
export class StepTimeout extends Error {
  constructor(
    public readonly step: string,
    public readonly timeoutMs: number,
  ) {
    super(`step '${step}' timed out after ${timeoutMs}ms`)
    this.name = 'StepTimeout'
  }
}

/**
 * The whole orchestration exceeded `maxDurationMs`. Retryable by default. §7.3.
 */
export class MaxDurationExceeded extends Error {
  constructor(public readonly maxDurationMs: number) {
    super(`orchestration exceeded maxDurationMs (${maxDurationMs}ms)`)
    this.name = 'MaxDurationExceeded'
  }
}

/**
 * The backend wired to this {@link Orchestrator}/queue does not implement the
 * journal capability (`readSteps`/`appendStep`/`completeClaimed`/
 * `heartbeatClaimed`). Thrown at construction.
 */
export class OrchestrationUnsupportedError extends Error {
  constructor(method: string, detail?: string) {
    super(
      detail ??
        `backend does not support durable orchestration (missing '${method}')`,
    )
    this.name = 'OrchestrationUnsupportedError'
  }
}

/** A type is registered as both an orchestrator and a plain processor. §3.4. */
export class OrchestratorTypeConflict extends Error {
  constructor(type: string) {
    super(`type '${type}' is already registered (orchestrator/processor clash)`)
    this.name = 'OrchestratorTypeConflict'
  }
}

/**
 * A `visibilityTimeoutMs` override looser than the queue's reaper timeout — the
 * lease could not actually be held. Startup error. §7.1.
 */
export class HeartbeatConfigConflict extends Error {
  constructor(overrideMs: number, reaperMs: number) {
    super(
      `visibilityTimeoutMs override (${overrideMs}ms) is looser than the queue reaper timeout (${reaperMs}ms); the lease cannot be held`,
    )
    this.name = 'HeartbeatConfigConflict'
  }
}

/**
 * Fatal orchestration errors map to `failFatal` (terminal, no retry). Everything
 * else (`StepTimeout`, `MaxDurationExceeded`, lease loss, plain throws) is a
 * normal retryable failure. §5.
 *
 * `OrchestrationUnsupportedError` is fatal: a backend that can't fence (e.g.
 * no claimToken minted on claim) will fail identically on every retry —
 * retrying only burns every attempt.
 */
export function isFatalOrchestrationError(err: unknown): boolean {
  return (
    err instanceof NonRetryable ||
    err instanceof NondeterminismError ||
    err instanceof NonSerializableStepResult ||
    err instanceof JournalTooLarge ||
    err instanceof OrchestrationUnsupportedError
  )
}
