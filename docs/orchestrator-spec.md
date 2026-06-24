# durabl Orchestrator — Durable Execution Layer (v1 Spec)

Status: Draft / ready-for-build once the journal/fatal-routing/lease-fencing
acceptance tests in §12 exist
Scope: v1 only. Durable timers, signals, fan-out helper, and version pinning are
explicitly deferred (see §10).

## 1. Problem

durabl today is **job-level durable only**: a handler that crashes halfway retries
from the top, re-running every side effect. For multi-step flows with external
mutations (Stripe customer + subscription + DB write), that means any
interruption can **double-charge, orphan a subscription, or duplicate a
customer**. (See Focuster `SubscriptionService.restartTrial` — cancel → ensure
customer → create subscription → mark → sync, with zero memory between steps.)

The Orchestrator adds **step-level durability**: completed steps are journaled and
never re-run on resume. The double-charge surface shrinks from "always, on any
retry" to **a single at-least-once window per step** — the interval between a
step's side effect landing and its journal write committing (one DB round-trip,
not "always"). That window is closed per dangerous step with an idempotency key
(§9). Steps are honestly **at-least-once, not exactly-once**; the design makes
the unsafe window small and gives you the tools to make it harmless, it does not
pretend the window is zero.

## 2. Model: resume-from-step (DBOS-style), NOT replay (Temporal-style)

We adopt DBOS-style **resume-from-last-step**, not Temporal-style full replay.

Consequences that shaped this design:

- **No sandbox, no isolate, no patched stdlib.** Orchestrator code runs in the
  normal Node process. `Date.now()`, `Math.random()`, `fetch`, and any
  third-party library calling them work normally — including deep inside step
  bodies, which run exactly once and are never replayed.
- **The only determinism rule:** do not let a nondeterministic value (clock,
  random, external state read outside a step) on the orchestrator control path
  decide **which `step()` runs next**. Nondeterminism *inside* a step body is fine
  (runs once, result frozen).
- Library uses of `Date.now()` (JWT `exp`, HTTP timeouts, cache TTL, ObjectId,
  backoff jitter) are **correct and untouched** — they want the real clock, and
  resume-from-step never re-runs them.

This is the key divergence from Temporal: Temporal must sandbox/ban the clock
because it replays the whole workflow body and can't tell library nondeterminism
from yours. We resume instead of replay, so we don't.

### 2.1 The determinism rule should be backed by tooling

The rule in §2 is the load-bearing safety property, and resume-from-step cannot
*mechanically* prevent a violation the way a replay sandbox can. We close the gap
with guardrails instead of trusting discipline. The runtime divergence detector is
v1; the dev-mode and lint guardrails below are follow-up work unless the first
production migration needs them:

- **`octx.now()` / `octx.uuid(label)` are provided and synchronous** (§4) precisely so
  the two most common nondeterminism sources never tempt a raw `await Date.now()`
  or `await randomUUID()` on the control path.
- **Dev-mode control-path guard.** When `NODE_ENV !== 'production'`, the future
  Orchestrator wraps the user fn and flags an `await` that resolves to a value the
  control flow then branches on *without* it having passed through `step()`,
  `now()`, or `uuid(label)`. This is a best-effort lint/heuristic (it cannot catch
  everything), logged as a loud warning naming the suspect call site. It exists to
  turn the most common foot-gun — `if (await fetchLiveState())` outside a step —
  into a visible warning in test/CI rather than a silent prod corruption.
- **Divergence detection (§6)** is the runtime backstop when a violation actually
  shifts which step runs at a given seq.

Branching on `job.data` (immutable) or on a journaled step result (frozen on
resume) is always safe. Branching on a non-journaled live read is the cardinal
sin; the guard above is how we make it loud.

### 2.2 Follow-up lint enforcement: `no-nondeterministic-control-path`

The strongest non-sandbox enforcement we can give is a lint rule that runs at
keystroke (editor), locally (`npm run lint`), and in CI. A follow-up `durabl/eslint`
subpath should ship an **opt-in ESLint plugin**. durabl core should not require a
linter; consumers who *write* orchestrators (where the foot-gun lives) add the rule
to their existing ESLint when they opt in:

```js
// eslint.config.js (flat config)
import durabl from 'durabl/eslint'
export default [
  ...durabl.configs.recommended,
  // equivalently:
  // { plugins: { durabl }, rules: { 'durabl/no-nondeterministic-control-path': 'error' } },
]
```

`eslint` and `@typescript-eslint/*` are **optional peer deps** — non-orchestrator
users never pull them. The rule needs only the TSESTree parser (no type
information / `parserOptions.project`), so it is fast and config-light.

**What it enforces (intentionally strict — the strictness is the auditability):**
inside an orchestrator body (a function whose 2nd param is typed
`OrchestratorContext`, or the 2nd arg of a `*.define(...)` call), the only thing you
may `await` is `octx.*` or `Promise.all` / `Promise.allSettled` over steps.
`Promise.race` and `Promise.any` are banned on the control path because their winner
depends on timing. The rule also flags direct `Date.now()`, `Math.random()`,
`new Date()`, and `randomUUID()` on the control path. Anything external belongs in
`octx.step(...)`, where it runs once and is journaled. Awaits and nondeterminism
**inside a step callback are exempt** — step bodies run exactly once and are never
replayed.

```typescript
orch.define('x', async (job, octx) => {
  if (await isFeatureEnabled('y')) { … }      // ❌ bareAwait — live read on control path
  const t = Date.now()                         // ❌ syncNondet — use octx.now()
  await db.users.save(job.data)                // ❌ bareAwait — wrap in octx.step()

  const sub = await octx.step('load', () => loadSub(job.data.id))  // ✅ journaled
  if (sub.active) { … }                        // ✅ branch on journaled result
  await octx.step('io', async () => {
    return (await fetch(url)).json()           // ✅ exempt — inside a step body
  })
})
```

**The ceiling (stated, not hidden):** this is a *local* AST rule. It cannot follow a
nondeterministic read hidden one function call away —
`if (await myHelper())` where `myHelper` does the live read — because that needs
whole-program taint analysis or a replay sandbox, both rejected by the
resume-from-step model (§2). The rule kills the common *inline* foot-gun at
keystroke; the divergence detector (§6) plus review remain the backstop for what it
structurally cannot see. It is a foot-gun reducer, not a proof — same honesty as
§2.1.

## 3. Architecture (decisions Q1–Q5)

### 3.1 Layer on top (Q1=B)
`Orchestrator` is a **separate class layered on `JobQueue`**, shipped in the
durabl package. It does **not** modify the load-bearing atomic-claim primitive.
Orchestration complexity is quarantined from the queue core.

### 3.2 Embedded journal (Q2=B)
The step journal is an **embedded array on the job document** (`job.steps[]`),
mirroring the existing `job.logs[]` pattern.

- A step-result write is a single-document conditional append → **inherently
  atomic, zero transactions.** The job stays `active` for the whole run; status
  only advances once at the end (`completeClaimed()`), already a separate
  single-doc write.
- **Tradeoff (accepted, but bounded — see §8.1):** step results accumulate
  against Mongo's 16MB BSON document cap **alongside `logs[]`** in the same
  document. This is a cumulative budget across the whole run, not a per-step or
  fan-out-only concern. `appendStep` enforces a running-size guard (§8.1) so the
  cap surfaces as a clear `JournalTooLarge` error, never an opaque mid-flow Mongo
  failure. A large fan-out or high step count would need the separate-collection
  variant — deferred (§10).

### 3.3 Step identity: seq-key + name-assert (Q3=C)
Each step is keyed by a **sequence number assigned synchronously at the moment
`octx.step()` is called** (not when its `fn` resolves). The step **name** is
recorded and asserted against the journal at that seq.

- Call-time (synchronous) seq assignment makes order deterministic even under
  concurrent dispatch (`Promise.all(items.map(id => octx.step(...)))` — `.map`
  invokes `octx.step` in array order before any await resolves).
- Seq-as-key handles loops (same name repeated at distinct seqs).
- Name-at-seq is the **divergence signal** (§6).

### 3.4 Execution reuses `process()` (Q4=A)
The Orchestrator registers a normal `JobQueue.process()` handler. The registered
handler is a wrapper:

```
(job, ctx) => {
  const octx = buildContext(job, ctx, await queue.readSteps(job.id))
  const heart = startHeartbeat(queue, job, config) // §7: self-scheduling, awaited, caught
  try {
    await withMaxDuration(                           // §7.3 orchestration-level cap
      userOrchestratorFn(job, octx),                 // normal throw → processJob.fail()
      config.maxDurationMs,
    )
    const done = await queue.completeClaimed(job.id, job.claimToken)
    if (done === 'lease-lost') throw new Error('orchestrator lease lost')
  } catch (err) {
    if (isFatalOrchestrationError(err)) {            // NonRetryable | NondeterminismError
      await ctx.failFatal(err.message)               // | NonSerializableStepResult | JournalTooLarge
      return
    }
    throw err
  } finally {
    heart.stop()
  }
}
```

All load-bearing machinery comes free: race-free `claimAndProcess` slot
reservation, exponential backoff, `recoverStuckJobs` reaper, push/poll pickup,
graceful shutdown. No new scheduling loop.

`define()` **refuses to register a type that already has a `process()` handler**
(and vice versa) — `JobQueue.process()` throwing "already registered" is surfaced
as a clear `OrchestratorTypeConflict`, so a stray double-registration is a
startup error, not order-dependent surprise.

### 3.5 Journal IO = optional backend capability (Q5=A)
Optional methods added to `IJobQueueBackend`, mirroring the existing optional
`onJobAvailable?` convention (detect-by-presence):

```typescript
interface IJobQueueBackend {
  // ...existing...
  readSteps?(jobId: string): Promise<StepRecord[]>
  appendStep?(jobId: string, claimToken: string, record: StepRecord): Promise<AppendStepResult>
  completeClaimed?(jobId: string, claimToken: string): Promise<CompleteClaimedResult>
  heartbeatClaimed?(jobId: string, claimToken: string): Promise<HeartbeatClaimedResult>
}
```

- `MongoJobQueue` implements them durably (`appendStep` = single-doc conditional
  append fenced on the claim token, `completeClaimed` = claim-token-only
  completion; append also bumps `claimedAt` — see §7).
- `DummyBackend` / `ImmediateBackend` implement in-memory versions so
  **orchestrator logic is unit-testable without Mongo** (preserves the core
  invariant "job logic must never need Mongo to test").
- `JobQueue` gets thin passthroughs delegating to the backend, so `Orchestrator`
  depends only on `JobQueue` and never reaches around it. Passthrough throws
  `OrchestrationUnsupportedError` if the backend lacks the capability.
  `Orchestrator` constructor asserts capability up front.

`StepRecord`:
```typescript
interface StepRecord {
  seq: number
  name: string
  result: unknown   // BSON-serializable (§8)
  ts: Date
}

type AppendStepResult = 'appended' | 'already-recorded' | 'lease-lost'
type CompleteClaimedResult = 'completed' | 'lease-lost'
type HeartbeatClaimedResult = 'heartbeated' | 'lease-lost'
```
`steps` is kept **off** the public `Job` type (no leak); it lives on `JobDoc`
and is read via `readSteps` internally.

### 3.6 Journal write semantics + lease fencing

`steps[]` is an append-only array for document locality, but callers must treat it
as a **seq-keyed journal**, not as an array index. `readSteps(jobId)` returns
records sorted by `seq` or the Orchestrator normalizes them into a
`Map<number, StepRecord>` before lookup. This is load-bearing: concurrent fan-out
can complete seq 3 before seq 2, so physical `$push` order is not execution order.

**Lease token, not attempt (R8).** Every claim mints a fresh random `claimToken`
(set in `claimNext`'s `findOneAndUpdate`, returned on the `Job`). All fencing —
`appendStep`, `completeClaimed`, `heartbeatClaimed` — filters on
`{ status: 'active', claimToken }`, not on `attempt`. `attempt` would *happen* to
work today (each claim `$inc`s it, so each active period has a unique value), but
that invariant is implicit and would silently break the moment any future path
re-activates a job without bumping `attempt` (e.g. an admin resume). A dedicated
per-claim nonce makes the fence explicit and robust by construction.

`appendStep` is idempotent and lease-aware:

- If the job is still `active`, still owned by the current `claimToken`, and no
  record with `record.seq` exists, append the record and set `claimedAt = now`.
- If a record with the same `seq` and `name` already exists, return
  `'already-recorded'`. The Orchestrator returns the recorded result instead of
  appending again. Covers driver-retry ambiguity after a successful write.
- If a record with the same `seq` but a different `name` exists, throw
  `NondeterminismError`.
- If the job is no longer `active` or the `claimToken` no longer matches, return
  `'lease-lost'`. The Orchestrator throws a normal retryable error and does not
  call `completeClaimed()`, so stale workers cannot finish a recovered job.

Final completion uses the same fence. The wrapper calls
`completeClaimed(job.id, job.claimToken)` rather than bare
`ctx.complete()`. If that returns `'lease-lost'`, the wrapper throws a normal
retryable error. This prevents a stale worker from completing a job the reaper
already returned to `pending` and another attempt may be running. On resume the
journaled steps all skip, so a re-run after a lost final complete re-executes only
the completion write, not the work.

### 3.7 Future operator introspection + manual resume (R9, D4)

A durable execution layer is only as good as its recovery story. These
operator-facing methods are useful, but they are follow-up work unless the first
production migration needs them:

```typescript
orch.inspect(jobId: string): Promise<JournalView | null>
orch.resume(jobId: string): Promise<'resumed' | 'not-resumable'>

interface JournalView {
  jobId: string
  type: string
  status: JobStatus
  steps: { seq: number; name: string; ts: Date }[]   // results omitted by default (may hold PII, §8.2)
  nextSeq: number                                     // where a resume would continue
  result?: unknown                                    // journaled final return, if completed
  failure?: { reason: string; fatal: boolean }
}
```

- `inspect` reads the journal for live ops: "which step is `restart-trial:user123`
  stuck on?" Step *results* are omitted from the default view because they may
  contain PII (§8.2); an explicit `{ includeResults: true }` opt-in returns them.
- `resume` would re-`pending` a `failed` job so it continues **from the journaled
  step**, not from scratch. This is the recovery path after a `NonRetryable` /
  divergence fatal leaves a half-applied external state: fix the root cause, then
  `resume` would continue from `nextSeq` rather than re-running (and re-firing)
  earlier mutations. `resume` mints a fresh `claimToken` on the next claim like any other
  pickup, so fencing is unchanged.

## 4. Public API (v1)

```typescript
const orch = new Orchestrator(queue)   // queue's backend must be journal-capable

orch.define<TData>(
  type: string,
  fn: (job: Job<TData>, octx: OrchestratorContext) => Promise<void>,
  config?: ProcessorConfig & {
    heartbeatIntervalMs?: number
    visibilityTimeoutMs?: number   // defaults to the queue's reaper timeout (§7.1) — do not desync
    stepTimeoutMs?: number         // per-step liveness cap (§7.2)
    maxDurationMs?: number         // whole-orchestration cap (§7.3)
  },
): void
```

`OrchestratorContext`:
```typescript
interface OrchestratorContext {
  // Memoized, journaled step. Idempotency key passed INTO fn; override at the call site.
  step<R>(
    name: string,
    fn: (keys: StepKeys, signal: AbortSignal) => Promise<R>,
    opts?: { idempotencyKey?: string; timeoutMs?: number },
  ): Promise<R>

  // SYNCHRONOUS deterministic helpers — backed by one journaled bootstrap record (§4.1).
  // No per-call DB round-trip, no viral await, no silent always-truthy Promise foot-gun.
  now(): number       // frozen logical start time of THIS run; identical on every call + resume
  uuid(label: string): string  // deterministic by label, stable across resume (NOT for secrets — §4.1)

  log(message: string): void
  heartbeat(): Promise<void>   // escape hatch; claim-token-fenced auto-heartbeat (§7) already runs
}

interface StepKeys {
  /** Default key for this step, stable across resumes of THIS job: `${jobId}:${seq}:${name}`.
   *  Defends "don't double-fire across retries of THIS job." Does NOT defend against a
   *  DIFFERENT job for the same entity — for that, pass `opts.idempotencyKey` with an
   *  entity-scoped value, or use `dedupeKey` on enqueue (§9). */
  idempotencyKey: string
  jobId: string
  seq: number
}
```

`NonRetryable` — error sentinel mapping a throw to `failFatal` (no retry, §5).

### 4.1 `now()` / `uuid(label)` are synchronous (D1)

These were async-returning journaled steps in the original draft. That cost a DB
round-trip per call, made every clock read `await`-viral, and created a silent
bug: a forgotten `await` yields an always-truthy `Promise`, so
`if (octx.now() > deadline)` takes the wrong branch with no error.

v1 instead captures a **single bootstrap record** (`{ startedAt, seed }`,
journaled once on the run's first use of `now`/`uuid`/`step`) and derives both
helpers synchronously:

- `now()` returns the frozen `startedAt` — the run's one logical "now," identical
  on every call and on every resume. (Need a *fresh* wall-clock reading mid-flow?
  That's an explicit `step('read-clock', () => Date.now())`.)
- `uuid(label)` derives from `seed` + `label`, not call order. The same label
  returns the same UUID across calls and resumes, **without** journaling each one.
  Labels must be unique for distinct values within a job; repeated labels are an
  intentional request for the same value. Job-namespaced by `seed`, so no cross-job
  collision. **Deterministic and therefore predictable** — fine for
  idempotency/correlation IDs, **not** for secrets or security tokens. For a
  crypto-random value that must be stable across resume, use a journaled
  `step('token', () => randomUUID())`.

### Canonical example (Focuster `restart-trial`)

```typescript
orch.define<{ userId: string }>('restart-trial', async (job, octx) => {
  const { userId } = job.data

  const existing = await octx.step('load-sub', () => getCurrentSubscription(userId))
  if (existing && !isExpired(existing.status)) {
    throw new NonRetryable(`bad state: ${existing.status}`)
  }

  if (existing) {
    await octx.step('cancel-old', () => stripeAdapter.cancelSubscription(existing.id))
  }

  // Entity-scoped key declared AT THE CALL SITE: a DIFFERENT job for the same user
  // must not mint a 2nd customer. The default jobId-scoped key would NOT cover this.
  const customerId = await octx.step(
    'ensure-customer',
    ({ idempotencyKey }) => ensureStripeCustomer(userId, { idempotencyKey }),
    { idempotencyKey: `${userId}:customer` },
  )

  const sub = await octx.step('create-sub', ({ idempotencyKey }) =>
    stripeAdapter.createSubscription(customerId, {
      plan: 'basic_monthly',
      trialDays: DEFAULT_TRIAL_DAYS,
      idempotencyKey,   // jobId-scoped default is correct here
    }),
  )

  await octx.step('mark-restarted', () =>
    Meteor.users.updateAsync(userId, { $set: { 'billing.trialRestarted': new Date(octx.now()) } }),
  )

  await octx.step('sync', () => new BillingService().sync(userId))

  octx.log(`restarted trial with subscription ${sub.id}`)
})
```

Enqueue is **unchanged** — an orchestrator is just a job type. The
entity-scoped `dedupeKey` here is the *first* line of defense (§9): it collapses a
double-click into one job before durability even matters.
```typescript
await queue.enqueue('restart-trial', { userId }, { dedupeKey: `restart-trial:${userId}` })
```

## 5. Step failure semantics (Q7=A)

- **Journal is append-on-success only.** A throwing step writes nothing → on
  resume it has no record → it re-runs. Only completed steps are journaled and
  skipped.
- A normal step throw **propagates** out of the orchestrator fn → `processJob`
  calls `backend.fail()` → existing jittered backoff → re-claim → resume.
  Job-level `maxAttempts` governs total attempts.
- **Fatal sentinels → `failFatal`** (terminal, no retry): `NonRetryable`,
  `NondeterminismError` (§6), `NonSerializableStepResult` (§8), `JournalTooLarge`
  (§8.1). The wrapper catches these, calls `ctx.failFatal(reason)`, logs
  structured context, stops the heartbeat, and returns without rethrowing. A
  fatal mid-flow leaves a half-applied external state by construction; v1 makes
  that state explicit in logs, while richer `inspect` / `resume` tooling is a
  follow-up (§3.7).
- A **step timeout** (§7.2) is a normal throw (retryable) by default, so a
  transient hang retries from resume rather than terminating the flow.
- No per-step retry policy in v1 (deferred §10).

## 6. Divergence detection (Q3, Q10)

On resume, when `octx.step(name)` is called at sequence `seq`:
- If `journalBySeq.get(seq)` exists and `.name !== name` →
  **`NondeterminismError` → `failFatal`** (terminal, Q10). Log `{ jobId, seq,
  expected: record.name, actual: name }`.
- If `journalBySeq.get(seq)` exists and name matches → return `record.result`
  (skip fn).
- If no record for `seq` → run fn, append result conditionally (§3.6), return.

Rationale for fatal: retrying re-runs the same nondeterministic/changed code →
same divergence → wastes attempts. Needs a human; future `orch.resume` tooling can
make that recovery path nicer (§3.7).

### 6.1 In-flight reshapes remain a v1 operational constraint

Reshaping an orchestrator's early steps while jobs of that type are in flight
makes those jobs divergence-fail on resume — historically a cryptic mid-run
`NondeterminismError`. v1 keeps this as an operational constraint: drain jobs of
that type before incompatible step reshapes, or accept that old in-flight jobs will
fatal on divergence and need manual recovery.

Do **not** stamp a version at claim time; a deploy can claim an old pending job with
new code and accidentally bless the wrong version. A future version-aware enqueue
path (`orch.enqueue(...)` or explicit `queue.enqueue(..., { orchestratorVersion })`)
can make this safe, but that is deferred (§10).

## 7. Heartbeat / lease (Q8=A+C) — hardened

`processJob` does not auto-heartbeat raw handlers. The Orchestrator wrapper does,
so users never manage leases. Three corrections from the original draft (R1, R2,
R3) make this safe instead of merely convenient.

### 7.1 Single source of truth for the visibility timeout (R3)

The lethal failure mode is the orchestrator's heartbeat interval and the reaper's
visibility timeout **disagreeing**: if the reaper reclaims a still-live job
mid-step, the in-flight (not-yet-journaled) step double-executes — the exact
double-charge this project exists to kill.

So `visibilityTimeoutMs` is **not a free-floating orchestrator config**. The
`JobQueue` owns and exposes its configured reaper timeout
(`queue.visibilityTimeoutMs`), and `recoverStuckJobs()` reads the same value. The
Orchestrator reads it from the queue and uses it unless explicitly overridden; an
override that is **looser** than the reaper timeout is a startup error
(`HeartbeatConfigConflict`), because it cannot actually hold the lease. Operators
cannot silently desync the two by editing one and forgetting the other.

`heartbeatIntervalMs` defaults to `visibilityTimeoutMs / 3`.

### 7.2 Self-scheduling, awaited, error-caught heartbeat (R2)

The original `setInterval(ctx.heartbeat, …)` is a broken lease-renewer:
`ctx.heartbeat` is async, `setInterval` doesn't await, so slow heartbeat writes
(exactly when the DB is under load) stack up into overlapping concurrent writes,
and a rejected heartbeat becomes an unhandled rejection with no retry.

v1 uses a self-scheduling loop instead:

```
function startHeartbeat(queue, job, config) {
  let stopped = false
  const tick = async () => {
    if (stopped) return
    try { await queue.heartbeatClaimed(job.id, job.claimToken) }
    catch (err) { log.warn({ err, jobId: job.id }, 'heartbeat failed; will retry next tick') }
    if (!stopped) timer = setTimeout(tick, config.heartbeatIntervalMs)
  }
  let timer = setTimeout(tick, config.heartbeatIntervalMs)
  return { stop() { stopped = true; clearTimeout(timer) } }
}
```

Never overlapping (next tick is scheduled only after the previous awaited),
never an unhandled rejection, always re-armed.

### 7.3 Step timeout + max duration — auto-heartbeat must not defeat the reaper (R1)

Auto-heartbeat is a double-edged sword: it stops the reaper from spuriously
reclaiming a legitimately slow step, but it also means a step that **hangs**
(a `fetch` with no timeout, a wedged external call) is heartbeated *forever* —
the job never progresses and the reaper, the base queue's liveness backstop,
never fires. Auto-heartbeat would convert "stuck job gets reaped and retried" into
"stuck job stuck forever."

Two caps restore liveness:

- **`stepTimeoutMs`** (per-step, overridable via `step(name, fn, { timeoutMs })`):
  a step that doesn't settle in time rejects with `StepTimeout` (a normal
  retryable throw, §5), so the flow fails out of the wedged call and retries from
  resume. There is **no** safe infinite default; if unset, `stepTimeoutMs` defaults
  to `visibilityTimeoutMs` so a hung step can never outlive a single lease period
  unnoticed. Timeouts are cooperative: the wrapper passes an `AbortSignal` into the
  step fn and aborts it when the timeout fires. Callers must thread that signal into
  cancellable APIs (`fetch`, drivers that support abort, etc.). If an external
  side effect cannot be cancelled, the timed-out operation may still land after the
  retry begins; that step remains at-least-once and needs an idempotency key (§9).
- **`maxDurationMs`** (whole orchestration): a backstop wrapping the entire user
  fn; on breach the wrapper stops, lets the heartbeat stop, and the job fails
  retryably (or fatally if you set it to, via `NonRetryable`). Guards against a
  pathological loop that keeps heartbeating across thousands of fast steps.

### 7.4 Progress bump on append (C)

`appendStep` also `$set: { claimedAt: now }` in the same single-doc write — a free
lease extension between steps, no extra round-trip. Note this only covers the gaps
*between* fast steps; a single long-running step holds the lease purely via §7.2's
heartbeat loop, which is why that loop (not the append bump) is the load-bearing
path for long steps.

## 8. Step result serialization (Q11)

- **Typing:** `step<R>` infers `R` from the fn return. On a journal hit the
  stored value is cast back to `R` — a documented trust boundary (TS can't verify
  what Mongo stored after a code change).
- **Constraint:** results must be **BSON-serializable** (plain objects, arrays,
  primitives, `Date`). No class instances, functions, `undefined`-valued object
  keys, cycles. Stripe SDK responses are plain JSON → fine.
- **Void steps:** `step<void>` is supported. The Orchestrator stores an internal
  completion sentinel for `undefined` top-level results and replays it as
  `undefined`. Only `undefined` nested inside returned objects remains invalid.
- **Runtime guard:** `appendStep` runs a cheap EJSON/structured-clone roundtrip
  check; on failure throws `NonSerializableStepResult` naming the step — before
  it corrupts the journal.

### 8.1 Cumulative size guard (R7)

The 16MB BSON cap is a *cumulative* per-job budget shared by `steps[]` **and**
`logs[]`, not a fan-out-only worry. Left unguarded, a long flow's append would
eventually hit an opaque Mongo error mid-flow, retry, re-hit it, and burn every
attempt to a confusing fatal. Instead, `appendStep` tracks the running serialized
journal size and throws `JournalTooLarge(step, approxBytes)` (fatal, §5) at a
configurable soft cap (default well under 16MB) — a clear, actionable failure
naming the offending step. Guidance unchanged: **return ids/refs, not whole
payloads** — keep results small.

### 8.2 Step results may contain PII — treat the journal as sensitive (S1)

Journaled step results are persisted **plaintext in the queue collection**,
next to `logs[]`. Billing flows journal Stripe responses → customer, payment, and
subscription metadata at rest in Mongo. Two rules:

- **Don't journal secrets** (raw card data, tokens, full PANs). Return references
  and re-fetch inside the next step if needed.
- Future `orch.inspect` should omit step results by default (§3.7); results should
  require an explicit `includeResults` opt-in, so casual ops introspection doesn't
  spray PII into logs.

Field-level encryption/redaction of results is deferred (§10) but the persistence
boundary is documented now so callers don't store what they shouldn't.

## 9. At-least-once steps — the honest guarantee

Steps are **at-least-once, not exactly-once.** Crash in the window between a step
fn returning and its `appendStep` committing (one DB round-trip, plus any fn code
after the external `await`) → the step re-runs on resume. This is the same
guarantee Temporal activities give. Close it per dangerous step:

1. **Idempotency key** — pass an entity-scoped `opts.idempotencyKey` at the call
   site (§4) when the default jobId-scope is too narrow; otherwise use the
   provided `StepKeys.idempotencyKey`. Stripe and most external APIs dedupe on it;
   internal Mongo mutations use upserts keyed by it. A double-fire in the crash
   window is absorbed.
2. **`dedupeKey` on enqueue** — kills the double-click / double-submit path before
   durability even matters (one job, not two). This is the *only* defense against
   two *different* jobs for the same entity; a jobId-scoped step key does not cover
   that case.

**Migration discipline:** for each *mutating* step, ask "idempotent?" If no, give
it a key, scoped to the entity that must not be duplicated. For Focuster billing
that's ~3 keys total (`create-sub` jobId-scoped, `ensure-customer` user-scoped,
the charge path in `createSubscription`). Read-only / naturally idempotent steps
(loads, cancels, upserts) need nothing. A future dev-mode guard should warn when a
step re-runs after a lost append **and has no idempotency key**, so unguarded
mutating steps surface in test rather than in a production double-charge.

## 10. Explicitly deferred (each a later bead)

| Item | Phase | Note |
|------|-------|------|
| `octx.sleep(duration)` durable timer | v2 | new `waiting` status + `wakeAt`, reaper wake |
| `octx.all(...)` fan-out ergonomic helper | v2 | works manually via `Promise.all` over steps in v1 |
| `octx.waitForSignal()` + `queue.signal()` | v3 | `signals` subdoc + parking |
| Version-aware enqueue / per-step version pinning | v4 | removes §6.1 in-flight-reshape limitation safely |
| Separate-collection journal | when needed | escape hatch for >16MB / high-step-count fan-out |
| Per-step retry policy | when needed | v1 is job-level retry only |
| Journal field-level encryption/redaction | when needed | §8.2 documents the boundary; v1 relies on "don't journal secrets" |
| ESLint plugin + dev-mode control-path heuristic | follow-up | useful guardrails; not required for the first durable-execution slice |
| `orch.inspect` / `orch.resume` | follow-up | operational recovery API; keep the journal shape compatible, but ship separately if needed |
| Final result journaling | follow-up | useful DX; not required for restart-trial-style side-effect flows |

## 11. v1 build order

1. `StepRecord` / `AppendStepResult` / `CompleteClaimedResult` types;
   `claimToken` minted in `claimNext` and threaded onto `Job`;
   `readSteps?`/`appendStep?`/`completeClaimed?`/`heartbeatClaimed?` on
   `IJobQueueBackend`; `MongoJobQueue` impl (claim-token-fenced conditional append
   + fenced final complete + `claimedAt` bump + same-seq duplicate handling +
   cumulative-size guard) + index review; in-memory impls on
   `DummyBackend`/`ImmediateBackend`.
2. `JobQueue` passthroughs + capability assertion + `visibilityTimeoutMs`
   exposure (§7.1) shared with `recoverStuckJobs()`.
3. `Orchestrator` class: `define` (with `process()` conflict guard), the wrapper
   handler, `OrchestratorContext` (`step` with `opts` + `AbortSignal`,
   synchronous `now`/`uuid(label)` via bootstrap record), seq assignment,
   divergence detector, append-on-success, serialization + size guards.
4. Hardened lease: self-scheduling awaited heartbeat (§7.2), `stepTimeoutMs` /
   `maxDurationMs` caps (§7.3), fatal-sentinel → `failFatal` mapping.
5. Tests (§12).
6. Focuster glue: `createOrchestrator()`; migrate `restart-trial` first as the
   proving flow; flip the Meteor method to enqueue.

## 12. Test plan (the toy→prod line)

The v1 module is a toy until these pass — acceptance criteria, not nice-to-haves:

- **Resume skips completed steps:** run orchestrator, kill after step N, re-claim,
  assert steps 1..N return journaled results and do NOT re-run (spy on side
  effects).
- **Crash between side-effect and append → at-least-once, absorbed by idem key:**
  assert the external mutation is called twice but the idempotency key is identical
  across both calls. Cover both the default jobId-scoped key and an explicit
  entity-scoped `opts.idempotencyKey`.
- **Divergence → failFatal:** journal a step name, replay with a different name at
  that seq, assert `NondeterminismError` + terminal `failed` + no retry.
- **`NonRetryable` → no retry; ordinary throw → retry-from-resume.**
- **Loop + fan-out seq stability:** `Promise.all(items.map(...))` resumes
  correctly; per-iteration idempotency keys are distinct and stable.
- **Timing-based combinators rejected:** `Promise.race` / `Promise.any` on the
  control path are lint failures; `Promise.all` / `allSettled` over steps are
  allowed.
- **Fan-out completion order:** make seq 3 resolve before seq 2, assert both are
  replayed by `seq` rather than physical append order.
- **Duplicate append ambiguity:** simulate `appendStep` returning
  `'already-recorded'` after the step fn ran, assert the Orchestrator returns the
  stored result and does not append a duplicate.
- **Stale worker fencing (claim token):** recover an active job, let the stale
  worker try to append/complete with its now-stale `claimToken`, assert both
  return `'lease-lost'`, the stale worker does not complete the job, and the
  recovered attempt continues. Include a case asserting the fence holds even if
  `attempt` collides (proves token-fencing, not attempt-fencing).
- **`now()` / `uuid(label)` determinism:** assert `now()` is constant across calls
  and across resume; assert `uuid('a')` regenerates identically on resume,
  `uuid('a') !== uuid('b')`, and values are distinct per job; assert no per-call
  append is written for either.
- **Synchronous-helper foot-gun is gone:** assert `now()`/`uuid(label)` return values
  (not Promises) — a forgotten-await regression test.
- **Void steps:** `step<void>` journals completion, skips on resume, returns
  `undefined`.
- **Serialization guard fires** on a non-BSON result with the step named.
- **Size guard fires:** an oversized cumulative journal throws `JournalTooLarge`
  naming the step, fatally, rather than an opaque Mongo error.
- **Heartbeat holds the lease (long step):** a step longer than the visibility
  timeout is not reaped mid-run when `visibilityTimeoutMs` is customized; assert no
  overlapping concurrent heartbeat writes and that a transient heartbeat rejection
  does not crash the run.
- **Heartbeat config conflict rejected:** a `visibilityTimeoutMs` override looser
  than the reaper timeout is a startup error.
- **Step timeout:** a hung step rejects with `StepTimeout` and retries from resume
  rather than heartbeating forever; assert the step fn receives an `AbortSignal`
  and that cancellable code observes `signal.aborted`.
- **Non-cancellable timeout honesty:** simulate a timed-out non-cancellable side
  effect that lands after timeout, assert the retry uses the same idempotency key
  and the docs/test name make the at-least-once behavior explicit.
- **Heartbeat token fencing:** stale worker heartbeat with an old `claimToken` does
  not refresh the recovered attempt's `claimedAt`.
- **`process()` / `define()` conflict:** registering the same type on both is a
  clear startup error.
- **Backend coverage:** pure orchestrator logic runs on in-memory journal-capable
  backends (no Mongo), including `DummyBackend`; `ImmediateBackend` coverage
  requires wiring `JobQueue.process()` registrations into its inline execution path
  or a test-only adapter. A subset runs on `MongoJobQueue` for
  durability/crash-resume/fencing.

## 13. Verdict

The *model* is prod-grade (DBOS proves it). The original draft delegated too much
reliability to operator discipline and deferred work; this revision moves the three
lethal items into v1 mechanism:

- **R3** heartbeat/reaper timeout can no longer desync (single source of truth).
- **R1/R2** auto-heartbeat no longer defeats the reaper (step/duration caps) and is
  a correct lease-renewer (self-scheduling, awaited, caught).
- **R8** fencing is on a per-claim token, not an incidentally-unique `attempt`.

With those plus §9 (idempotency keys on the 2–3 mutating steps), §6 (divergence
detection), and §12 (crash-under-failure tests), the implementation is
prod-credible — none of it research, all leveraging durabl's existing atomic-claim /
lease / heartbeat / reaper primitives. Strictly better than Focuster's current
retry-from-top billing the moment `restart-trial` ships behind it.

---

## Changelog

### v1.1 — reliability + DX hardening pass

Revisions from the v1 draft, prompted by an API-DX / reliability review. Each maps
to a finding ID (R = reliability, D = DX, S = security).

- **R1 — step/duration caps so auto-heartbeat can't defeat the reaper.** Added
  `stepTimeoutMs` (per-step, default = `visibilityTimeoutMs`) with cooperative
  `AbortSignal` cancellation, plus `maxDurationMs` (whole orchestration). Previously
  a hung step was heartbeated forever and never reaped. (§5, §7.3, §10, §12)
- **R2 — fixed the lease-renewer.** Replaced `setInterval(ctx.heartbeat)` with a
  claim-token-fenced, self-scheduling `setTimeout` loop that awaits each write,
  catches rejections, and re-arms — no overlapping writes, no unhandled rejections.
  (§3.4, §7.2)
- **R3 — single source of truth for the visibility timeout.** `JobQueue` owns and
  exposes the reaper timeout; the Orchestrator reads it; a looser override is a
  startup error (`HeartbeatConfigConflict`). Kills the desync-by-doc-sentence
  split-brain. (§7.1, §12)
- **R4 — honest reliability messaging.** §1 no longer claims a "sub-millisecond
  crash window"; it states at-least-once with a one-DB-round-trip window, aligned
  with §9. (§1, §9)
- **R5 — determinism rule gets planned guards.** Proposed a dev-mode control-path heuristic
  that warns on a branch over a non-journaled `await`, plus the synchronous
  `now()`/`uuid(label)` helpers to remove the temptation. (§2.1)
- **R5b — lint enforcement.** Proposed `durabl/eslint`'s
  `no-nondeterministic-control-path` rule (opt-in plugin, optional peer deps):
  bans bare `await`/sync-nondeterminism on an orchestrator control path at
  keystroke + CI, exempts step-callback bodies. Closes the common *inline*
  foot-gun; cross-function-boundary hides remain the documented ceiling. (§2.2)
- **R6 / D2 — idempotency is first-class.** `step()` takes an explicit
  `opts.idempotencyKey`; canonical example uses an entity-scoped key for
  `ensure-customer` (the default jobId-scoped key did not cover the cross-job
  hazard); a future dev-mode guard can warn when a keyless mutating step re-runs
  after a lost append. (§4, §4 example, §9, §12)
- **R7 — cumulative journal size guard.** `appendStep` tracks running serialized
  size (`steps[]` + `logs[]`) and throws `JournalTooLarge(step, approxBytes)` below
  the 16MB cap, instead of an opaque mid-flow Mongo failure. (§3.2, §8.1, §12)
- **R8 — fence on a per-claim lease token, not `attempt`.** `claimNext` mints a
  random `claimToken`; `appendStep` / `completeClaimed` / `heartbeatClaimed` fence on it.
  Removes the implicit "every active period has a unique attempt" coupling. (§3.5,
  §3.6, §11, §12)
- **R9 / D4 — recovery + introspection API.** Moved `orch.inspect(jobId)` and
  `orch.resume(jobId)` to follow-up scope so v1 can ship the core durable execution
  mechanics first. (§3.7, §10)
- **D1 — `now()` / `uuid(label)` are synchronous.** Backed by a single journaled
  bootstrap record (`{ startedAt, seed }`). Removes the per-call DB round-trip, the
  viral `await`, and the forgotten-await always-truthy-Promise bug. (§4.1, §12)
- **D3 — final result journaling deferred.** Returning orchestrators and
  inspect-readable final results are useful DX, but not required for the first
  restart-trial-style migration. (§10)
- **D5 — registration conflict guard.** `define()` and `process()` on the same type
  is a clear `OrchestratorTypeConflict` startup error. (§3.4, §12)
- **D6 — version stamping deferred.** The spec now explicitly rejects claim-time
  version stamping and leaves version-aware enqueue / per-step pinning for a later
  phase. (§6.1, §10)
- **S1 — journal-as-sensitive boundary documented.** "Don't journal secrets,"
  future `inspect` omits results by default, field-level encryption deferred.
  (§8.2, §10)
