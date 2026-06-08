# AGENTS.md

Guidance for AI coding agents working in this repository.

## Commands

```bash
npm test                  # vitest run (all suites)
npm run test:watch        # vitest watch mode
npx vitest run test/MongoJobQueue.test.ts          # single file
npx vitest run -t "claims the next pending job"     # single test by name
npm run typecheck         # tsc --noEmit (strict; noUnusedLocals/Params on)
npm run build             # tsup → dist/ (esm + cjs + dts)
```

`mongodb` is a **peer** dependency. `tsup` marks it `external` — never bundle it.

### Test infra

Mongo-backed suites boot an in-memory single-node replica set via `mongodb-memory-server` (first run downloads a `mongod` binary). Point at a real cluster instead:

```bash
MONGO_URL="mongodb://localhost:27017/?replicaSet=rs0" npm test
```

`vitest.config.ts` sets `fileParallelism: false` (serial — avoids oplog contention) and long timeouts (30s test / 60s hook). The change-stream suite self-skips when `MONGO_URL` is a standalone (non-replica-set). Shared boot/teardown lives in `test/mongoHelper.ts`.

## Architecture

Two layers, split on purpose:

- **`JobQueue` (`src/JobQueue.ts`)** — the public API + the processor poll loop. Backend-agnostic. Manages per-type `ProcessorState` (concurrency caps, exponential backoff `1s→60s` on errors), graceful `shutdown`, and the push→poll wiring.
- **`IJobQueueBackend` (`src/backends/IJobQueueBackend.ts`)** — the storage contract. Three implementations:
  - `MongoJobQueue` — production. Atomic claim via `findOneAndUpdate` (the Mongo equivalent of `SELECT … FOR UPDATE SKIP LOCKED`).
  - `DummyBackend` — records calls, executes nothing. Unit-test job-producing code without Mongo.
  - `ImmediateBackend` — runs handlers inline on enqueue. Integration tests that want side effects without a poll loop.

**Job logic must never need Mongo to test.** That's the whole reason the backend is an interface — keep it that way when extending.

### Key invariants (don't break these)

- **Atomic claim** is the core primitive. Two workers must never claim the same job. Any change to the claim query/sort in `MongoJobQueue` is load-bearing.
- **Dedupe** is enforced by a unique *partial* index (`src/backends/mongoJobIndexes.ts`), not application logic. Two scopes:
  - `pending+active` (default) — blocks any duplicate.
  - `pending` — allows one pending behind one active = single-flight coalescing. This is what `claimOrEnqueue` uses to replace a distributed lock.
- **Leases, not deletes.** A claimed job is `active` with a visibility timeout. Handlers `heartbeat()` to extend; a reaper returns dead-worker jobs to `pending`.
- **Job-level durability only.** No step replay. A handler that crashes halfway retries from the top. Don't add checkpointing without a deliberate decision — it's explicitly out of scope.

### Push/poll hybrid (`MongoChangeStreamWatcher`)

`onJobAvailable?` is an **optional** backend method with three distinct states callers must honor (documented in full on the interface):

1. **omitted** — no push (e.g. `DummyBackend`); poll at default interval.
2. **present, returns `null`** — push capable but disabled (`useChangeStreams` off); fall back to polling.
3. **present, returns unsubscribe fn** — push active; bump poll to the 60s safety-net (`PUSH_POLL_INTERVAL_MS`) and lean on the stream.

A change-stream reconnect emits an **empty-string `''`** sentinel — a catch-up nudge meaning "re-poll any registered processor", not a real job type. Callers must treat `''` accordingly. The poll loop always stays alive as a safety net even when push is active.

## Conventions

- `verbatimModuleSyntax` + `isolatedModules` are on — use `import type` / `export type` for type-only imports (see `src/index.ts`).
- Inject a `Logger` (`src/logger.ts`); don't `console.log`. Default is `consoleLogger`; pass a pino/winston instance in prod.
- README is the canonical API doc and reflects real Focuster production usage. Keep it in sync when the public surface shifts.
