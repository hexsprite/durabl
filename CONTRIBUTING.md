# Contributing to durabl

## Setup

```bash
npm ci
npm test
```

The first `npm test` downloads a `mongod` binary (~100MB) — the Mongo-backed
suites boot an in-memory single-node replica set via `mongodb-memory-server`. A
replica set rather than a standalone because change streams require one.

To run against a real cluster instead:

```bash
MONGO_URL="mongodb://localhost:27017/?replicaSet=rs0" npm test
```

The change-stream suite self-skips when `MONGO_URL` points at a standalone.

## The gates

```bash
npm run typecheck   # tsc --noEmit (strict, noUnusedLocals/Params on)
npm run lint        # eslint, zero warnings tolerated
npm test            # vitest run
npm run build       # tsup → dist/ (esm + cjs + dts)
```

All four must pass. The lint gate is `--max-warnings 0` on purpose: a warning
nobody has to fix is a warning everybody stops reading.

Rules the compiler already owns (`no-undef`, `no-unused-vars`) are switched off
in `eslint.config.ts` — `tsc` is the authority, and two tools reporting the same
thing differently is worse than one.

`eslint.config.ts` imports the plugin straight from `src/eslint/` via `jiti`, so
linting never depends on a prior `npm run build`.

## Test layout

`vitest.config.mts` sets `fileParallelism: false`. The Mongo suites contend on
the oplog, and parallel files produced failures that depended on machine speed
rather than on the code. Timeouts are correspondingly long (30s test, 60s hook).

Shared boot/teardown lives in `test/mongoHelper.ts`. Use `uniqueCollectionName()`
so suites cannot collide.

### The chaos and property harnesses

`test/queueProperties.test.ts` generates randomised operation sequences and
checks the claim/dedupe/lease invariants after every operation.
`test/queueChaos.test.ts` injects faults (drop, throw, duplicate, delay) at the
backend seam.

Both use a **fixed seed** by default, so a failure is reproducible and CI cannot
go intermittently red for reasons nobody can chase. That matters more than it
sounds: a genuine bug once arrived as an unreproducible "flake" and was nearly
dismissed as one.

To search wider:

```bash
CHAOS_SEED=12345 CHAOS_RUNS=500 npx vitest run test/queueProperties.test.ts
CHAOS_SEEDS=1,2,3,4,5 npx vitest run test/queueChaos.test.ts
```

`test/model/harness.test.ts` tests the harness itself — that the invariant
checker catches each violation it claims to, that the PRNG is deterministic, and
that the fault injector actually injects. A harness that cannot fail is worse
than no harness, because it reports green forever while guarding nothing.

If a harness finds a bug, add a **named** regression test alongside it. The
generative test proves the absence of a class; the named test documents the
specific bug and its symptom so future-you knows why the guard exists.

## Conventions

- `verbatimModuleSyntax` and `isolatedModules` are on — use `import type` /
  `export type` for type-only imports.
- Inject a `Logger` (`src/logger.ts`); never `console.log`.
- Comments explain **why**, not what. The existing ones note things like why the
  claim index key order is load-bearing and why `matchedCount` is checked instead
  of `modifiedCount` — that is the bar.
- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).
- Fix a bug, add a regression test. Name it in behavioural terms and note the
  symptom. If the bug crossed a module boundary, test at each layer it touched.

## Invariants that are not up for casual change

Read `AGENTS.md` before touching these, and expect a design discussion:

- **Atomic claim.** Two workers must never claim the same job. Any change to the
  claim query or its sort in `MongoJobQueue` is load-bearing, and the index key
  order exists for a measured reason.
- **Dedupe lives in unique partial indexes**, not application logic. Enforcing it
  in code reintroduces the check-then-act race the indexes exist to close.
- **Leases, not deletes.** A claimed job is `active` with a visibility timeout.
- **Job logic must be testable without Mongo.** That is the entire reason the
  backend is an interface. If an in-memory backend is more permissive than Mongo,
  the tests certify a lie — this has happened twice and both were real bugs.

## Peer dependency

`mongodb` is a **peer** dependency and `tsup` marks it external. Never bundle it:
two copies of the driver in one process is its own class of bug.
