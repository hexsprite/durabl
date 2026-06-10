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
// else: someone is already running and one run is queued behind them.
```

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

## API sketch

```typescript
class JobQueue {
  enqueue<T>(type, data, options?): Promise<string | null>
  claimOrEnqueue<T>(type, data, options?): Promise<JobHandle<T> | null>
  process<T>(type, handler, config?): void
  getStats(type?): Promise<QueueStats>
  startup(): Promise<void>
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
```

The handler receives a `JobContext` with `complete()`, `fail(reason)`, `failFatal(reason)`, `log(message)`, and `heartbeat()`.

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

- Not a workflow engine. No step-level checkpointing or replay. If a handler crashes halfway, the whole job retries from the top.
- Not multi-datastore. MongoDB only, for now. The backend interface would accommodate a Postgres implementation (`FOR UPDATE SKIP LOCKED` maps cleanly), and that may land later.
- Not battle-tested as a standalone package. The *queue* has years of production behind it; the *npm package* does not. File issues.

## License

MIT © Jordan Baker
