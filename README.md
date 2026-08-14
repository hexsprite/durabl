# durabl

[![CI](https://github.com/hexsprite/durabl/actions/workflows/ci.yml/badge.svg)](https://github.com/hexsprite/durabl/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/durabl.svg)](https://www.npmjs.com/package/durabl)
[![license](https://img.shields.io/npm/l/durabl.svg)](./LICENSE)

A small durable job queue backed by MongoDB. Atomic claiming, retries, visibility-timeout leases, dedupe keys, and optional change-stream push — no Redis, no separate worker service, no orchestrator.

> **Status: work in progress.** This is the production job queue I've run inside [Focuster](https://focuster.com) since 2016, just lifted out of the app and decoupled from Meteor. It works and it's tested, but the packaging is young: the API may still shift and the docs are thin in places. Treat `0.x` as "useful, not yet stable."

## Why this exists

Focuster needed a durable queue for calendar sync jobs when Meteor 3 landed and the old `vsivsi:job-collection` package stopped working. I looked at the obvious options first:

- **Temporal.** Full workflow orchestration with deterministic replay. Powerful, and far more machinery than four job types need. It also wants a server to run.
- **DBOS.** Lovely API, durable workflows checkpointed to Postgres. But it's Postgres, and Focuster's system of record is MongoDB. Adding a second datastore to run background jobs is a tax I didn't want to pay.
- **BullMQ.** The default answer in Node land, but it needs Redis. Same objection: new infrastructure for a problem the existing database already solves.
- **Agenda, Keuss, Pulse.** The MongoDB-native options were either stale, archived, or missing features I relied on like priorities and atomic dedupe.

The actual workload is modest: a handful of job types at concurrency 2–16, polling every few seconds, with one hard requirement — **don't run the same user's sync twice at once**, even across a rolling deploy. MongoDB's `findOneAndUpdate` is exactly the primitive that solves atomic claiming, and a unique partial index solves dedupe. So the queue is ~900 lines of TypeScript over the `mongodb` driver instead of a dependency on Redis or a workflow engine.

I stole the good ideas (pluggable backends from Django's task framework, the dedupe-key concept from BullMQ/SQS) and skipped the heavy ones (step-level replay from Inngest/DBOS — job-level durability is enough for now).

## Features

- **Atomic claim.** `findOneAndUpdate` claims pending, due jobs by priority. Two workers never claim the same job.
- **Queue-managed leases.** The queue heartbeats each managed run and aborts it if renewal stays unconfirmed through the lease deadline. The reaper returns jobs from dead workers to `pending`.
- **Managed retries.** A successful handler return completes the job. An `Error` retries or fails at `maxAttempts`. `FatalJobError` fails immediately and preserves its cause.
- **Delayed and prioritized scheduling.** `runAt` delays a job. Lower `priority` numbers run first.
- **Dedupe keys, two scopes.** `pending+active` blocks all duplicates. `pending` allows one pending follower behind one active run.
- **Latest-payload coalescing.** `coalesce: 'latest'` replaces the pending follower payload while the active payload stays immutable.
- **Push/poll hybrid.** MongoDB change streams provide fast pickup. The poll loop remains a safety net.
- **Pluggable backends.** `MongoJobQueue` is the production backend. `DummyBackend` records calls. `ImmediateBackend` runs `queue.process` handlers during `enqueue`.

## Install

```bash
npm install durabl mongodb
```

`mongodb` is a peer dependency — durabl uses your driver instance and version.

## Quickstart

```typescript
import { MongoClient } from 'mongodb'
import { JobQueue, MongoJobQueue } from 'durabl'

const client = await MongoClient.connect(process.env.MONGO_URL!)
const db = client.db('app')

// Create and start the backend. startup() creates the indexes.
const backend = new MongoJobQueue({ db })
await backend.startup()

const queue = new JobQueue(backend)

// Register a managed processor.
queue.process<{ userId: string }>(
  'welcome-email',
  async (job, ctx) => {
    await sendWelcomeEmail(job.data.userId, { signal: ctx.signal })
    ctx.log('welcome email sent')
    // Return to complete the job.
  },
  { concurrency: 4, pollInterval: 5000 },
)

// The dedupe key blocks another pending or active job for this user.
const jobId = await queue.enqueue(
  'welcome-email',
  { userId: 'u_123' },
  { dedupeKey: 'welcome-email:u_123' },
)
if (jobId === null) {
  // A job for this user is already pending or active.
}
```

The queue owns the claim lifecycle. It heartbeats the job while the handler runs.
If renewal stays unconfirmed through the local lease deadline, it aborts `ctx.signal` and suppresses stale terminal writes.
A successful return completes the job. Throw an `Error` to retry the job.
Throw `FatalJobError` when another attempt cannot succeed.
`JobContext` contains only `signal` and `log(message)`.

### Change streams (push pickup)

Pass `useChangeStreams: true` to get near-instant pickup instead of waiting for the next poll. This requires a replica set (MongoDB Atlas provides one; a single-node `rs` works for local dev).

```typescript
const backend = new MongoJobQueue({ db, useChangeStreams: true })
await backend.startup() // throws if the server isn't a replica set
```

When push is active, `JobQueue` bumps its default poll interval to 60s and leans on the stream for latency, keeping the poll loop only as a safety net for dropped events and crash recovery.

### Reaper (stuck-job recovery)

A claimed job is a lease. The reaper recovers a job after its visibility timeout expires.
Start the reaper on one process and inspect the immediate recovery:

```typescript
const result = await queue.startReaper()
if (result.status === 'started') {
  reportReaperStartup(result.recovered)
}
```

`recovered` is the number of jobs recovered by the startup sweep. It is `null` if that sweep failed.
Periodic sweeps continue after a failed startup sweep. A later call returns `{ status: 'already-running' }`.

Set `visibilityTimeoutMs` in the `JobQueue` constructor. The queue uses this value for managed heartbeats and reaper sweeps.
The timer does not keep the process alive. `stopReaper()` and `shutdown()` stop it.

`backend.recoverStuckJobs(visibilityTimeoutMs)` remains a low-level API for tests and one-off operations.
Do not schedule it with another timeout value.

### Inline execution with coalescing

Use `claimOrEnqueue` when a request must run now and only one run can own a key.
Pass the returned handle to `runClaimed`. Do not manage its lifecycle directly.

```typescript
const handle = await queue.claimOrEnqueue(
  'reschedule',
  { userId, requestedAt: new Date() },
  {
    dedupeKey: `reschedule:${userId}`,
    dedupeScope: 'pending',
    coalesce: 'latest',
  },
)

if (handle) {
  await queue.runClaimed(handle, async (job, ctx) => {
    await reschedule(job.data, { signal: ctx.signal })
  })
}
```

The active payload never changes. If a pending follower exists, `coalesce: 'latest'` replaces its payload.
Without `coalesce`, the pending follower keeps the first payload. This is the legacy behavior.
Use latest coalescing only for replaceable workloads.

After a successful return, `runClaimed` claims pending jobs with the same type and dedupe key.
If a handler fails but a pending follower already covers its work, Durabl marks the old job `superseded`, runs the follower, then throws the original handler error.
`maxDrains` limits the number of additional claims across both paths. Its default is 10, so one call can run at most 11 jobs.
Set `{ maxDrains: 0 }` to disable follower draining.

`claimOrEnqueue` returns `null` when another job already holds the dedupe slot:

| Scope | Active run exists | Pending follower exists |
| --- | --- | --- |
| `'pending'` | Creates one follower, then returns `null` | Returns `null`. Latest coalescing replaces its payload. Legacy behavior keeps its first payload. |
| `'pending+active'` | Returns `null` without creating a follower | Returns `null` without creating another job. |

The unique partial indexes enforce this result across processes. Do not use a state read as a coordination lock.

## Migrating to 0.3.0

Version 0.3.0 is a breaking minor release under the `0.x` version policy.

### Processor migration

1. Remove calls to `ctx.complete()`, `ctx.fail()`, `ctx.failFatal()`, and `ctx.heartbeat()`.
2. Return from the handler after successful work.
3. Throw an `Error` when the queue can retry the job.
4. Throw `FatalJobError` when another attempt cannot succeed.
5. Use `ctx.signal` to cancel external work. Use `ctx.log(message)` for job logs.
6. Replace `ImmediateBackend.registerHandler` with `queue.process`. Await `queue.enqueue` to await inline processing.

Import `FatalJobError` from `durabl`.

The queue now heartbeats all managed runs. It derives each terminal transition from the handler outcome.

### Inline migration

Replace each manual handle loop with one managed call:

```typescript
const handle = await queue.claimOrEnqueue(type, data, options)
if (handle) await queue.runClaimed(handle, handler)
```

Remove manual heartbeat, completion, failure, and follower-drain calls.
Use `maxDrains` only when the default of 10 additional same-key claims is not suitable.

### Reaper migration

Await `startReaper()` and inspect its result:

```typescript
const result = await queue.startReaper()
if (result.status === 'started') reportRecovered(result.recovered)
```

### Custom backend migration

Custom backends must add these methods and result types:

```typescript
claimNextByKey<T>(
  type: string,
  dedupeKey: string,
): Promise<JobHandle<T> | null>

complete(jobId: string, claimToken?: string): Promise<CompleteJobResult>
fail(
  jobId: string,
  reason: string,
  claimToken?: string,
): Promise<FailJobResult>
failFatal(
  jobId: string,
  reason: string,
  claimToken?: string,
): Promise<FailFatalJobResult>
release(jobId: string, claimToken?: string): Promise<ReleaseJobResult>
hasOutstanding(type: string, dedupeKey: string): Promise<boolean>
```

Claim tokens must fence lifecycle writes.
Terminal methods must not overwrite an existing terminal state.
They must return these result objects:

| Method | Result objects |
| --- | --- |
| `complete` | `{ status: 'completed' }`, `{ status: 'already-terminal', terminalStatus }`, `{ status: 'lease-lost' }`, or `{ status: 'not-found' }` |
| `fail` | `{ status: 'retry-scheduled' }`, `{ status: 'failed-terminal' }`, `{ status: 'superseded' }`, `{ status: 'already-terminal', terminalStatus }`, `{ status: 'lease-lost' }`, or `{ status: 'not-found' }` |
| `failFatal` | `{ status: 'failed-terminal' }`, `{ status: 'already-terminal', terminalStatus }`, `{ status: 'lease-lost' }`, or `{ status: 'not-found' }` |
| `release` | `{ status: 'released' }`, `{ status: 'superseded' }`, `{ status: 'already-terminal', terminalStatus }`, `{ status: 'lease-lost' }`, or `{ status: 'not-found' }` |

`terminalStatus` is `'completed'`, `'failed'`, or `'superseded'`.
`heartbeat` still returns `'applied'` or `'lease-lost'`.
Store terminal receipts by claim token and operation so a later claim cannot erase an ambiguous write's result.
For `coalesce: 'latest'`, replace only the pending follower payload.
Never replace the active payload.
These direct lifecycle APIs exist for migration and backend integration. Application handlers must use managed execution.

## Testing your jobs

The backend is an interface, so job logic does not need MongoDB in a unit test.

```typescript
import { DummyBackend, JobQueue } from 'durabl'

const backend = new DummyBackend()
const queue = new JobQueue(backend)

await myService.doThing()

expect(backend.jobs).toHaveLength(1)
expect(backend.jobs[0].dedupeKey).toBe('thing:42')
```

`ImmediateBackend` uses the same `queue.process` API as production.
Its `enqueue` call waits until the managed handler finishes.

```typescript
import { ImmediateBackend, JobQueue } from 'durabl'

const backend = new ImmediateBackend()
const queue = new JobQueue(backend)

queue.process('welcome-email', sendWelcomeEmailJob)
await queue.enqueue('welcome-email', { userId: 'u_123' })
```

### Testing orchestrations (`durabl/testing`)

Orchestrator bodies unit-test against the real step machine over an in-memory journal — no queue, no backend, no timers:

```typescript
import { testOrchestration } from 'durabl/testing'

const t = testOrchestration(restartTrial, { data: { userId: 'u1' } })

await t.crashAfter('create-sub') // worker dies right after the step commits
await t.resume()                 // fresh attempt, same journal

expect(stripeMock.createSubscription).toHaveBeenCalledTimes(1) // replayed, not re-run
expect(t.steps.map((s) => s.name)).toEqual(['create-sub', 'sync'])
```

- `crashAfter(name)` — crash after the step's result is journaled; `resume()` skips it.
- `crashBefore(name)` — crash after the side effect but before the journal write; `resume()` re-runs the step. This is the window per-step idempotency keys defend — assert your key dedupes.
- `resumeWith(fn)` — replay the journal against a *different* body; assert it rejects with `NondeterminismError` (or doesn't) to prove a body edit is replay-compatible before it ships.
- `steps`, `logs`, `attempts` — journaled steps (decoded), `octx.log` lines, attempt count.

`octx.now()`/`octx.uuid()` are deterministic by default (fixed bootstrap seed), so step results are snapshot-stable. Serialization guards run on every append — a non-BSON-serializable step result fails the unit test the same way it would fail in production.

## API sketch

```typescript
declare class JobQueue {
  constructor(backend: IJobQueueBackend, logger?: Logger, options?: JobQueueOptions)
  enqueue(type: string, data: unknown, options?: EnqueueOptions): Promise<string | null>
  claimOrEnqueue<T>(type: string, data: T, options?: EnqueueOptions): Promise<JobHandle<T> | null>
  runClaimed<T>(handle: JobHandle<T>, handler: JobHandler<T>, options?: RunClaimedOptions): Promise<void>
  process<T>(type: string, handler: JobHandler<T>, config?: ProcessorConfig): void
  hasOutstanding(type: string, dedupeKey: string): Promise<boolean>
  getStats(type?: string): Promise<QueueStats>
  startup(): Promise<void>
  startReaper(intervalMs?: number): Promise<StartReaperResult>
  stopReaper(): void
  shutdown(timeoutMs?: number): Promise<void>
  installSignalHandlers(options?: { signals?: NodeJS.Signals[]; timeoutMs?: number }): () => void
  get draining(): Promise<void> | null
}

type JobHandler<T> = (job: Job<T>, ctx: JobContext) => void | Promise<void>
declare class FatalJobError extends Error {
  constructor(message: string, options?: ErrorOptions)
}


interface JobContext {
  signal: AbortSignal
  log(message: string): void
}

interface RunClaimedOptions {
  maxDrains?: number // additional same-key claims. default 10
}

interface ProcessorConfig {
  concurrency?: number
  pollInterval?: number
  heartbeatIntervalMs?: number
}

interface EnqueueOptions {
  priority?: number
  delay?: number
  maxAttempts?: number
  dedupeKey?: string
  dedupeScope?: 'pending' | 'pending+active'
  coalesce?: 'latest'
  backoff?: 'exponential' | 'fixed'
  backoffDelay?: number
  backoffMaxDelay?: number
}

type StartReaperResult =
  | { status: 'started'; recovered: number | null }
  | { status: 'already-running' }

interface JobHandle<T> extends Job<T> {
  status: 'active'
  complete(): Promise<CompleteJobResult>
  fail(reason: string): Promise<FailJobResult>
  failFatal(reason: string): Promise<FailFatalJobResult>
  heartbeat(): Promise<'applied' | 'lease-lost'>
  release(): Promise<ReleaseJobResult>
  log(message: string): void
}

type AlreadyTerminalJobResult = {
  status: 'already-terminal'
  terminalStatus: 'completed' | 'failed' | 'superseded'
}

type CompleteJobResult =
  | { status: 'completed' }
  | AlreadyTerminalJobResult
  | { status: 'lease-lost' }
  | { status: 'not-found' }

type FailJobResult =
  | { status: 'retry-scheduled' }
  | { status: 'failed-terminal' }
  | { status: 'superseded' }
  | AlreadyTerminalJobResult
  | { status: 'lease-lost' }
  | { status: 'not-found' }

type FailFatalJobResult =
  | { status: 'failed-terminal' }
  | AlreadyTerminalJobResult
  | { status: 'lease-lost' }
  | { status: 'not-found' }

type ReleaseJobResult =
  | { status: 'released' }
  | { status: 'superseded' }
  | AlreadyTerminalJobResult
  | { status: 'lease-lost' }
  | { status: 'not-found' }

interface QueueStats {
  pending: number
  active: number
  completed: number
  failed: number
  superseded: number
  oldestPendingRunAt: Date | null
  oldestPendingLagMs: number
}
```

Handlers receive a plain `Job` and a `JobContext`. They never receive lifecycle methods.
`JobHandle` is a low-level migration and backend integration API.
Pass a direct handle to `runClaimed` for normal application work.

### Outstanding state

`hasOutstanding(type, dedupeKey)` reports whether a matching job is pending or active.
The result is advisory because another process can change the state immediately.
Use it for health checks, status displays, and diagnostics.
Do not use it for check-then-enqueue coordination. Dedupe writes provide that guarantee.

### Global queue configuration

`setGlobalBackend` captures the backend, logger, and queue options for the default queue.
`createJobQueue` uses that configuration and applies explicit overrides.

```typescript
interface JobQueueOptions {
  visibilityTimeoutMs?: number
  onJobEvent?: JobEventSink
}

type GlobalQueueOptions = JobQueueOptions & { logger?: Logger }

function setGlobalBackend(
  backend: IJobQueueBackend,
  options?: GlobalQueueOptions,
): void

function getDefaultQueue(): JobQueue

function createJobQueue(
  backend?: IJobQueueBackend,
  overrides?: GlobalQueueOptions,
): JobQueue

function withGlobalQueue<T>(
  backend: IJobQueueBackend,
  callback: () => T | Promise<T>,
): Promise<T>
function withGlobalQueue<T>(
  backend: IJobQueueBackend,
  options: GlobalQueueOptions,
  callback: () => T | Promise<T>,
): Promise<T>

setGlobalBackend(backend, {
  logger,
  visibilityTimeoutMs: 300_000,
  onJobEvent,
})

const defaultQueue = getDefaultQueue()
const workerQueue = createJobQueue()
const otherQueue = createJobQueue(otherBackend, {
  visibilityTimeoutMs: 60_000,
})
```

Use `withGlobalQueue` for a temporary global scope:

```typescript
await withGlobalQueue(testBackend, { logger: testLogger }, async () => {
  await getDefaultQueue().enqueue('test-job', {})
})
```

Pass the callback as the second argument to use default queue options.

`withGlobalQueue` rejects overlapping scopes.
It restores the exact prior backend, options, and queue objects.
It shuts down only the temporary queue.

### Metrics

`getStats()` is a point-in-time gauge; `onJobEvent` is the stream. Pass it a
sink and you get counters and histograms without parsing logs:

```typescript
const queue = new JobQueue(backend, logger, {
  onJobEvent: (e) => {
    switch (e.kind) {
      case 'completed':
        statsd.timing('jobs.duration', e.durationMs, { type: e.type })
        statsd.increment('jobs.completed', { type: e.type })
        break
      case 'failed':
        // `terminal` is the distinction worth charting: a flaky job that
        // recovers on retry is not an incident, a give-up is.
        statsd.increment(e.terminal ? 'jobs.dead' : 'jobs.retried', {
          type: e.type,
        })
        break
      case 'superseded':
        statsd.increment('jobs.superseded', { type: e.type })
        break
      case 'lease-lost':
        statsd.increment('jobs.lease_lost', { type: e.type, op: e.op })
        break
      case 'reaper-recovered':
        statsd.gauge('jobs.reaper.recovered', e.handled)
        if (e.saturated) statsd.increment('jobs.reaper.saturated')
        break
    }
  },
})
```

Kinds: `claimed`, `completed`, `failed`, `superseded`, `fail-fatal`,
`lease-lost`, `reaper-recovered`, `reaper-error`, and `shutdown-released`.
The sink is synchronous and fire-and-forget. A sink error does not affect the job.

Don't build dashboards by matching durabl's log messages. Those strings are not
an API and will change.

### Deployment: drain on SIGTERM

Install the signal handlers, or call `shutdown()` from the host shutdown hook:

```typescript
queue.installSignalHandlers({ timeoutMs: 20_000 })
```

`shutdown()` fences new claims and managed runs before it snapshots active work.
`claimOrEnqueue()` and `runClaimed()` reject after shutdown starts. If a backend claim loses the race, Durabl releases or supersedes it before rejecting.
Shutdown waits for managed runs until the timeout. It then aborts each remaining `ctx.signal` and stops its heartbeat.
It releases each remaining claim to the due pending queue without consuming an attempt.
If a pending follower blocks release, shutdown marks the old claim `superseded` because the follower already preserves the work.
Release writes have a separate one-second bound, so a broken backend cannot block shutdown.

The timeout must fit inside the platform kill deadline.
If the platform sends `SIGKILL` after 30 seconds, use a timeout of less than 30 seconds.

`installSignalHandlers` does not call `process.exit()`.
Await `queue.draining`, or call `shutdown()` from the host lifecycle.
The returned uninstall function removes only the listeners that Durabl added.

### What to alert on

Alert on **`oldestPendingLagMs`**, not on `pending`.

Depth cannot tell a healthy queue from a stuck one. A backlog of 5 that has been
waiting 40 minutes is an incident; a backlog of 5000 draining fast is normal
Monday-morning traffic. Lag distinguishes them, and it is the metric that goes
red for every cause worth waking up for — dead workers, a wedged handler, a
processor that was never registered, a poll loop stalled behind contention.

```typescript
const { pending, oldestPendingLagMs } = await queue.getStats('pushSync')
if (oldestPendingLagMs > 5 * 60_000) pageSomeone({ pending })
```

Jobs scheduled for the future are excluded on purpose: something deliberately
delayed until next week is scheduled, not late. Counting it would peg the metric
permanently red, and a metric that is always red is one nobody looks at.

## Durable orchestration (step-level resume)

Job-level durability retries a crashed handler **from the top**. For multi-step side-effect flows (billing, provisioning) that's a double-charge waiting to happen. The opt-in `Orchestrator` adds DBOS-style **step-level** durability: completed steps are journaled on the job document and *skipped* on resume, so a crash re-runs only the unfinished tail.

It's a thin layer over `JobQueue` — an orchestrator is just a job type. The atomic-claim primitive is untouched. Requires a journal-capable backend (`MongoJobQueue`, or the in-memory test backends).

```typescript
import { Orchestrator, NonRetryable } from 'durabl'

const orch = new Orchestrator(queue) // queue's backend must be journal-capable

orch.define<{ userId: string }>('restart-trial', async (job, octx) => {
  const { userId } = job.data

  // Each octx.step runs once, is journaled, and returns the cached result on resume.
  const existing = await octx.step('load-sub', () => getCurrentSubscription(userId))
  if (existing && !isExpired(existing.status)) {
    throw new NonRetryable(`bad state: ${existing.status}`) // terminal, no retry
  }

  // Idempotency key is passed INTO the step fn. Default is jobId-scoped; override
  // at the call site for an entity-scoped key (a *different* job for the same user
  // must not mint a 2nd customer).
  const customerId = await octx.step(
    'ensure-customer',
    ({ idempotencyKey }) => ensureStripeCustomer(userId, { idempotencyKey }),
    { idempotencyKey: `${userId}:customer` },
  )

  const sub = await octx.step('create-sub', ({ idempotencyKey }) =>
    stripe.createSubscription(customerId, { plan: 'basic', idempotencyKey }),
  )

  octx.log(`restarted trial with subscription ${sub.id}`)
})

// Enqueue is unchanged — the entity-scoped dedupeKey collapses double-clicks first.
await queue.enqueue('restart-trial', { userId }, { dedupeKey: `restart-trial:${userId}` })
```

`OrchestratorContext`:

```typescript
interface OrchestratorContext {
  // Memoized, journaled step. Idempotency key passed in; override via opts.
  step<R>(name, fn: (keys, signal: AbortSignal) => Promise<R>, opts?: {
    idempotencyKey?: string
    timeoutMs?: number   // per-step liveness cap; default = visibilityTimeoutMs
  }): Promise<R>

  // Both backed by ONE journaled bootstrap record ({ startedAt, seed }), captured
  // lazily on the run's first use and read back on resume.
  now(): number          // frozen at the FIRST attempt's start (not enqueue time); identical on every resume
  uuid(label: string): string  // derived from the journaled seed + label; stable across resume (NOT for secrets)
  log(message: string): void
  // Escape hatch; auto-heartbeat already runs. 'lease-lost' means another
  // worker reclaimed the job — the run signal aborts and the next step() throws.
  heartbeat(): Promise<'heartbeated' | 'lease-lost'>
  // Run-level abort: fires on lease loss or maxDurationMs breach. step()
  // refuses to start once aborted; thread it into long non-step work.
  signal: AbortSignal
}
```

Guarantees and limits worth knowing:

- **At-least-once steps, not exactly-once.** A crash in the window between a step fn returning and its journal append committing re-runs that step. Close it on dangerous steps with the provided idempotency key (Stripe et al. dedupe on it) or a `dedupeKey` on enqueue.
- **Determinism on the control path.** Only `await` `octx.*` (or `Promise.all`/`allSettled` over steps) in the orchestrator body; do live reads / `Date.now()` / randomness *inside* a step. The optional `durabl/eslint` rule enforces this; the divergence detector (a changed step name at a journaled seq → fatal `NondeterminismError`) is the runtime backstop.
- **Auto-managed lease.** The wrapper heartbeats on a self-scheduling loop sized to the queue's reaper timeout (`visibilityTimeoutMs`, the single source of truth). `stepTimeoutMs` and `maxDurationMs` keep a hung step from heartbeating forever. When a heartbeat comes back `'lease-lost'` (the job was reclaimed) the loop stops and `octx.signal` aborts — the orphaned body throws at its next `step()` boundary instead of firing side effects.
- **Journal lives on the job doc**, inside Mongo's 16MB budget. Return ids/refs, not whole payloads; an oversized journal fails with a clear `JournalTooLarge`. Treat journaled results as sensitive (don't journal secrets). `logs[]` shares the document but not the budget — it's bounded to the newest `maxLogEntries` (default 1000, each message clipped at `maxLogMessageBytes`), so log volume can neither trip a spurious `JournalTooLarge` nor grow the document to the point where the write that marks a job failed no longer fits.

Full design and rationale: [`docs/orchestrator-spec.md`](docs/orchestrator-spec.md).

## Running the tests

```bash
npm install
npm test
```

The Mongo-backed suites spin up an in-memory single-node replica set via [`mongodb-memory-server`](https://github.com/typegoose/mongodb-memory-server) (the first run downloads a `mongod` binary). To test against a real cluster instead, point it at one:

```bash
MONGO_URL="mongodb://localhost:27017/?replicaSet=rs0" npm test
```

The change-stream suite self-skips if `MONGO_URL` points at a standalone (non-replica-set) server.

## Operating it

**Run the reaper on one process.** Await `queue.startReaper()` and inspect the immediate recovery result.
Without a reaper, a dead worker can hold a dedupe key forever.
Several reapers are safe because sweeps are idempotent, but they add redundant work.

**Configure the visibility timeout in one place.** Set `visibilityTimeoutMs` on `JobQueue`.
The queue sizes managed heartbeats from this value and passes it to `startReaper()`.
Do not schedule `backend.recoverStuckJobs(...)` with another value.

**Enqueue everywhere, process where intended.** Construct the backend on every instance that enqueues jobs.
Call `process()` only on worker instances.

**Return or throw from handlers.** A successful return completes the job.
An `Error` schedules a retry or terminal failure. `FatalJobError` records a terminal failure immediately.
Use `ctx.signal` to stop external work after shutdown or lease loss.

**Drain on SIGTERM.** A bounded shutdown releases managed claims that exceed the grace period.
This prevents a deploy from leaving those claims active until the visibility timeout.

**Turning on change streams changes pickup latency for scheduled jobs.** With
push active the poll interval becomes a 60s safety net, so a job with a future
`runAt` can fire up to 60s late. Fine for most work; surprising if you were
relying on the 5s default.

## Further reading

- [`docs/orchestrator-spec.md`](docs/orchestrator-spec.md) — the full design of
  the durable-execution layer: why resume-from-step rather than replay, step
  identity, journal write semantics and lease fencing, the failure taxonomy, and
  what is deliberately deferred.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — test setup, the chaos and property
  harnesses, and the invariants that are not up for casual change.
- [`CHANGELOG.md`](CHANGELOG.md)
- [`examples/quickstart.mjs`](examples/quickstart.mjs) — runnable end-to-end.

## What this is not

- Not a workflow engine by default — base `JobQueue` handlers retry from the top. Step-level resume is available as the opt-in [`Orchestrator`](#durable-orchestration-step-level-resume) layer (journaled steps, skipped on resume), but there are no timers, signals, or fan-out combinators yet (deferred — see the spec).
- Not multi-datastore. MongoDB only, for now. The backend interface would accommodate a Postgres implementation (`FOR UPDATE SKIP LOCKED` maps cleanly), and that may land later.
- Not battle-tested as a standalone package. The *queue* has years of production behind it; the *npm package* does not. File issues.

## License

MIT © Jordan Baker
