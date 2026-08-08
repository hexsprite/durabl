# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x`, breaking changes ship in minor releases and are
called out under **BREAKING** below.

## [Unreleased]

### Added

- `QueueStats.oldestPendingLagMs` / `oldestPendingRunAt` — backlog **age**, not
  just depth. This is the signal to alert on: a backlog of 5 waiting 40 minutes
  is an incident, 5000 draining fast is normal. Jobs scheduled for the future are
  excluded, so a deliberately delayed job cannot peg the metric red. Served by
  the existing claim index on Mongo; no new index.
- `JobQueue.installSignalHandlers()` — drain in-flight jobs on SIGTERM/SIGINT.
  Opt-in, never calls `process.exit()`, and removes only the listeners it added.
  Skipping a drain is expensive and invisible: jobs die mid-handler, sit `active`
  until the visibility timeout, burn an attempt, and re-run their side effects.
- `onJobEvent` sink on `JobQueueOptions` — lifecycle events for metrics
  (`claimed`, `completed`, `failed`, `fail-fatal`, `lease-lost`,
  `reaper-recovered`). `failed.terminal` separates a retry from a give-up;
  `reaper-recovered.saturated` flags a sweep that hit its batch cap. Emitted from
  `JobQueue`, so every backend gets it without changing `IJobQueueBackend`.
- Property-based and fault-injection test harnesses over the claim, dedupe and
  lease invariants. Both run on every PR; `CHAOS_SEED`/`CHAOS_RUNS`/`CHAOS_SEEDS`
  widen the search locally.

### Fixed

- **`claimNext` no longer throws on a dedupe-blocked candidate.** With
  `dedupeScope: 'pending'`, a pending job queued behind an active one made
  `claimNext` violate `dedupe_active_idx` and surface `E11000`. `claimAndProcess`
  read that as backend failure and applied exponential backoff (1s → 60s) to the
  ProcessorState — which is keyed on job *type* — so a single contended key
  stalled the poll loop for every other key of that type, escalating, while
  logging errors on a healthy queue. Blocked candidates are now skipped.
- **A transient failure could wedge a job forever.** When an active job with
  `dedupeScope: 'pending'` failed with retries remaining, `fail()` wrote
  `status: 'pending'` and collided with the queued follow-up on
  `dedupe_pending_idx`. `processJob` swallowed the throw, leaving the job
  `active`; the reaper's requeue path performed the same write and threw
  identically, so *nothing* could recover it. The job held its dedupeKey
  permanently, which under `pending+active` blocked every future job for that
  key. Both paths now retire the job as coalesced — the queued follow-up already
  covers the work.
- `DummyBackend.claimNext` enforces dedupe exclusivity and honours `runAt`. It
  previously ignored both, so a unit test could pass against it while the same
  logic misbehaved in production — which defeats the reason the backend exists.
- The step-divergence check (§6) had three independent implementations; folded
  into one shared `assertStepMatches` predicate.

### Changed

- `vitest` 2 → 4, and all npm advisories cleared (6 → 0). Includes an `overrides`
  entry pinning a patched `esbuild`, which neither `tsup` nor `vite` could reach.
- CI actions bumped off the deprecated Node 20 runtime.
- `ImmediateBackend` ignoring `delay` is now a documented decision rather than an
  accident — running inline on enqueue is the point, and honouring a delay would
  make it neither immediate nor useful.

## [0.2.1] - 2026-08-07

### Added

- `durabl/testing` — a crash/resume harness for exercising orchestrator bodies
  without hand-rolling failure injection.

## [0.2.0] - 2026-08-06

### Added

- **Durable orchestration** (`Orchestrator`): DBOS-style step-level resume.
  Completed steps are journaled on the job document and skipped on resume, so a
  crash re-runs only the unfinished tail. Job-level retry alone re-runs a handler
  from the top, which for multi-step side-effect flows is a double-charge waiting
  to happen.
- `durabl/eslint` — opt-in plugin with `no-nondeterministic-control-path`, which
  catches the determinism violations the resume model depends on.
- Journal capability as an optional backend contract, with claim-token fenced
  lifecycle writes so a zombie worker cannot mutate a reclaimed job.

### Changed

- Step machine narrowed onto a `StepJournalPort` interface.

### Fixed

- Dedupe, lease, shutdown, log and reaper hardening. Notably: `logs[]` is bounded
  structurally so a chatty handler cannot grow a document to the 16MB cap and
  thereby break the *terminal* write that records why the job failed.
- Concurrency slot is reserved before the claim `await`, so a burst of push
  notifications cannot overshoot the per-instance cap.
- Jittered retry backoff, so a fast-failing handler cannot burn every attempt in
  milliseconds and a downstream outage does not become an instant-retry storm.

## [0.1.1] - 2026-06-12

### Changed

- Ship CJS-first so CommonJS bundlers (Meteor) consume the package cleanly.
- Align `package.json` with the published dual-package layout.

## [0.1.0] - 2026-06-10

### Added

- Initial release. Mongo-backed durable job queue: atomic claim via
  `findOneAndUpdate`, retries, visibility-timeout leases, dedupe keys with
  `pending` / `pending+active` scopes, and change-stream push with a poll-loop
  safety net.

[unreleased]: https://github.com/hexsprite/durabl/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/hexsprite/durabl/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/hexsprite/durabl/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/hexsprite/durabl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/hexsprite/durabl/releases/tag/v0.1.0
