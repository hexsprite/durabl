# durabl

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

- **Atomic claim.** `findOneAndUpdate` on pending, due jobs sorted by priority. The MongoDB equivalent of `SELECT ... FOR UPDATE SKIP LOCKED`: two workers never claim the same job.
- **Visibility-timeout leases.** A claimed job is leased, not removed. Handlers heartbeat to extend the lease, and a reaper returns jobs from dead workers to `pending`.
- **Retries with attempt caps and backoff.** Failed jobs go back to `pending` with a jittered backoff delay until `maxAttempts`, then land in a terminal `failed` state. `failFatal()` skips retries for unrecoverable errors.
- **Delayed and prioritized scheduling.** `runAt` delays a job; lower `priority` numbers run first.
- **Dedupe keys, two scopes.** `pending+active` blocks any duplicate. `pending` allows one pending behind one active, which gives you single-flight coalescing: run now, queue at most one more.
- **Push/poll hybrid.** Rides MongoDB change streams for sub-100ms pickup, with a reconnect catch-up sentinel so jobs that land during a stream blip aren't missed. Degrades cleanly to polling when change streams are off or unavailable.
- **Deploy-ordered startup hooks.** `createDeployGate` skips index/validator/migration work when an older image boots beside a newer one during a rolling deploy, so the old machine can't revert the new schema.
- **Pluggable backends.** One interface, three implementations: `MongoJobQueue` for production, plus `DummyBackend` (records calls) and `ImmediateBackend` (runs inline) for tests. Swap the backend and test your job logic without mocking Mongo.

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

// 1. Create and start the backend (creates indexes).
const backend = new MongoJobQueue({ db })
await backend.startup()

// 2. Wrap it in a queue.
const queue = new JobQueue(backend)

// 3. Register a processor.
queue.process<{ userId: string }>(
  'welcome-email',
  async (job, ctx) => {
    await sendWelcomeEmail(job.data.userId)
    await ctx.complete()
  },
  { concurrency: 4, pollInterval: 5000 },
)

// 4. Enqueue. The dedupeKey makes this idempotent: a second enqueue while
//    the first is still pending/active returns null instead of duplicating.
const jobId = await queue.enqueue(
  'welcome-email',
  { userId: 'u_123' },
  { dedupeKey: 'welcome-email:u_123' },
)
if (jobId === null) {
  // A job for this user is already queued — nothing to do.
}
```

### Change streams (push pickup)

Pass `useChangeStreams: true` to get near-instant pickup instead of waiting for the next poll. This requires a replica set (MongoDB Atlas provides one; a single-node `rs` works for local dev).

```typescript
const backend = new MongoJobQueue({ db, useChangeStreams: true })
await backend.startup() // throws if the server isn't a replica set
```

When push is active, `JobQueue` bumps its default poll interval to 60s and leans on the stream for latency, keeping the poll loop only as a safety net for dropped events and crash recovery.

### Reaper (stuck-job recovery)

A claimed job is a lease, not a delete — if a worker dies mid-job, the reaper returns the job to `pending` (or terminal `failed` once attempts are exhausted) after its visibility timeout expires. Start it on one process:

```typescript
const queue = new JobQueue(backend, logger, { visibilityTimeoutMs: 300000 })
queue.startReaper() // sweeps every 60s; queue.startReaper(intervalMs) to tune
```

`startReaper()` always sweeps with the queue's configured `visibilityTimeoutMs`, which is the single source of truth: the `Orchestrator` sizes its heartbeats from the same value, so the lease window a handler maintains and the window the reaper enforces can never drift apart. The timer is `unref`'d and stops on `shutdown()` (or `stopReaper()`).

`backend.recoverStuckJobs(visibilityTimeoutMs)` remains public for manual/one-off sweeps and tests, but don't schedule it yourself with a hand-passed value — if it disagrees with the queue's, jobs get reaped out from under live workers (or dead workers hold leases too long).

### Inline execution with coalescing

For the "run it now, but never run two at once, and coalesce a burst into at most one follow-up" pattern (this replaced a 300-line distributed lock in Focuster), use `claimOrEnqueue` with `dedupeScope: 'pending'`:

```typescript
const handle = await queue.claimOrEnqueue(
  'reschedule',
  { userId },
  { dedupeKey: `reschedule:${userId}`, dedupeScope: 'pending' },
)

if (handle) {
  // We won the slot — run inline, no poll delay.
  try {
    await reschedule(userId)
    await handle.complete()
  } catch (err) {
    await handle.fail(String(err)) // poll loop will retry
  }
}
// else: someone is already running, and this request has been coalesced into
// the single follow-up queued behind them — the poll loop will run it.
```

Exactly what `null` means, per scope:

| scope | a run is already active | a run is already queued |
| --- | --- | --- |
| `'pending'` (single-flight) | `null`; one follow-up is queued behind it, and a burst collapses into that same one | `null`; nothing queued (one is enough) |
| `'pending+active'` (default) | `null`; nothing queued — this scope means "no duplicate at all" | `null`; nothing queued |

Mutual exclusion is enforced by unique partial indexes, not by a pre-read, so
concurrent callers across a rolling deploy get the same answer as sequential
ones: exactly one wins.

### Deploy-ordered startup hooks

A rolling deploy overlaps machines, so an *older* image can boot beside a newer one. Startup work that runs unconditionally on every boot — index creation, `collMod` validators, migrations — gets re-applied by whichever process boots last, and the old machine silently reverts the new schema.

`createDeployGate` records the newest build any process has seen in a singleton document and turns the older machine's hooks into a no-op:

```typescript
import { createDeployGate } from 'durabl'

const runIfLatestBuild = createDeployGate({ db, revision: process.env.GIT_SHA })

runIfLatestBuild(async () => { await reconcileIndexes() })
runIfLatestBuild(async () => { await runMigrations() })

// Once, at the end of startup. Hooks run in registration order.
const { ran } = await runIfLatestBuild.run()
```

The build version defaults to the mtime of `process.argv[1]` — cheap, monotonic per deploy, no build-time codegen. That's the fallback, not the contract: pass `buildTimestamp` to drive it from a real build identity, or `entrypoint` to point the stat somewhere else. A throwing hook propagates and the recorded version is left alone, so a half-finished deploy never claims to be the latest.

**It's a skip, not a lock.** Two processes carrying the *identical* build timestamp both run their hooks; nothing serialises them. That's deliberate — the problem is ordering, not exclusion, and hooks have to be idempotent anyway since they run on every boot of the newest build. A lock on the startup path is a new way to hang a boot, so it isn't taken by default.

For one hook, `runIfLatestBuild(options, hook)` does the create/register/run in a single call.

## Testing your jobs

The backend is an interface, so your job logic never has to touch Mongo in a unit test.

```typescript
import { DummyBackend, JobQueue } from 'durabl'

const backend = new DummyBackend() // records, doesn't execute
const queue = new JobQueue(backend)

await myService.doThing() // calls queue.enqueue under the hood

expect(backend.jobs).toHaveLength(1)
expect(backend.jobs[0].dedupeKey).toBe('thing:42')
```

`ImmediateBackend` runs handlers synchronously on enqueue, which is handy for integration tests where you want side effects without a poll loop.

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
class JobQueue {
  enqueue<T>(type, data, options?): Promise<string | null>
  claimOrEnqueue<T>(type, data, options?): Promise<JobHandle<T> | null>
  process<T>(type, handler, config?): void
  getStats(type?): Promise<QueueStats>
  startup(): Promise<void>
  startReaper(intervalMs?): void   // sweep with the queue's visibilityTimeoutMs
  stopReaper(): void
  shutdown(timeoutMs?): Promise<void>
}

interface EnqueueOptions {
  priority?: number       // lower = higher priority. default 0
  delay?: number          // ms before claimable. default 0
  maxAttempts?: number    // default 3
  dedupeKey?: string
  dedupeScope?: 'pending' | 'pending+active' // default 'pending+active'
  // Retry backoff — spaces failed attempts so a fast-failing handler can't
  // burn every attempt in milliseconds, and an outage doesn't become an
  // instant-retry storm.
  backoff?: 'exponential' | 'fixed' // default 'exponential' (full jitter)
  backoffDelay?: number   // base/floor ms. default 1000
  backoffMaxDelay?: number // cap ms. default 60000
}

interface ProcessorConfig {
  concurrency?: number    // default 1
  pollInterval?: number   // default 5000 (60000 when change streams are on)
}

function createDeployGate(options: DeployGateOptions): DeployGate

interface DeployGateOptions {
  db: Db
  collectionName?: string // default 'deployments'
  deploymentId?: string   // singleton _id. default 'default'
  buildTimestamp?: Date   // explicit build version; wins over entrypoint
  entrypoint?: string     // path whose mtime is the version. default process.argv[1]
  revision?: string       // git SHA / image digest, recorded alongside
  logger?: Logger
}

interface DeployGate {
  (hook: () => Promise<void>): void  // register; runs in registration order
  run(): Promise<DeployGateResult>   // { ran, buildTimestamp, previousTimestamp, hooksRun }
}
```

The handler receives a `JobContext` with `complete()`, `fail(reason)`, `failFatal(reason)`, `log(message)`, and `heartbeat()`.

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

## What this is not

- Not a workflow engine by default — base `JobQueue` handlers retry from the top. Step-level resume is available as the opt-in [`Orchestrator`](#durable-orchestration-step-level-resume) layer (journaled steps, skipped on resume), but there are no timers, signals, or fan-out combinators yet (deferred — see the spec).
- Not multi-datastore. MongoDB only, for now. The backend interface would accommodate a Postgres implementation (`FOR UPDATE SKIP LOCKED` maps cleanly), and that may land later.
- Not battle-tested as a standalone package. The *queue* has years of production behind it; the *npm package* does not. File issues.

## License

MIT © Jordan Baker
