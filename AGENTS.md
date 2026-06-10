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

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:970c3bf2 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->

<!-- BEGIN BEADS CODEX SETUP: generated by bd setup codex -->
## Beads Issue Tracker

Use Beads (`bd`) for durable task tracking in repositories that include it. Use the `beads` skill at `.agents/skills/beads/SKILL.md` (project install) or `~/.agents/skills/beads/SKILL.md` (global install) for Beads workflow guidance, then use the `bd` CLI for issue operations.

### Quick Reference

```bash
bd ready                # Find available work
bd show <id>            # View issue details
bd update <id> --claim  # Claim work
bd close <id>           # Complete work
bd prime                # Refresh Beads context
```

### Rules

- Use `bd` for all task tracking; do not create markdown TODO lists.
- Run `bd prime` when Beads context is missing or stale. Codex 0.129.0+ can load Beads context automatically through native hooks; use `/hooks` to inspect or toggle them.
- Keep persistent project memory in Beads via `bd remember`; do not create ad hoc memory files.

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.
<!-- END BEADS CODEX SETUP -->
