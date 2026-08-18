# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x`, breaking changes ship in minor releases and are
called out under **BREAKING** below.

## [Unreleased]

## [0.3.1] - 2026-08-17

### Changed

- Retryable claim and reaper-sweep failures log at `warn`, not `error`. A
  failure that the backoff retries is not an error. A sustained outage still
  surfaces: after 5 consecutive failures the site logs `error` once. The count
  is per processor type for claims, and resets on the first success. The error
  object stays on the log line at both levels.

## [0.3.0] - 2026-08-14

### Added

- `JobQueue.runClaimed()` provides queue-managed execution for an existing claim.
  It completes successful handlers, records handler errors, drains same-key
  followers, and heartbeats the lease.
- `FatalJobError` records an immediate terminal failure without another retry.
- `superseded` terminal status, statistics, receipts, cleanup, and lifecycle events distinguish replaced work from failures.
- `coalesce: 'latest'` replaces the payload of the one pending follower.
  Omitting it preserves the first follower payload.
- `RunClaimedOptions.maxDrains` bounds additional same-key claims. The default is
  10 additional claims after the initial claim.
- `JobQueue.hasOutstanding()` reports advisory pending or active state for one
  type and dedupe key.
- `JobQueue.startReaper()` runs an immediate recovery sweep and returns its
  asynchronous result before it starts periodic sweeps.
- `GlobalQueueOptions`, `setGlobalBackend()`, `createJobQueue()`, and both
  `withGlobalQueue(backend, callback)` and
  `withGlobalQueue(backend, options, callback)` provide configured scopes.
- `release()` returns a live claim to the due pending queue. If a pending
  follower blocks release, it marks the old claim `superseded`.
- `QueueStats.oldestPendingLagMs`, `oldestPendingRunAt`, and `superseded`
  provide concrete backlog and retirement metrics.
- `JobQueue.installSignalHandlers()` drains in-flight jobs on SIGTERM and
  SIGINT. It never calls `process.exit()`.
- `JobQueueOptions.onJobEvent` emits lifecycle events for metrics. New events report reaper errors and shutdown releases.
- `ProcessorConfig.heartbeatIntervalMs` overrides the managed lease-renewal
  cadence without mutating the handler function.
- Property-based and fault-injection harnesses cover claim, dedupe, and lease
  invariants.

### Changed

- `JobQueue` now owns handler completion, failure, heartbeat, lease loss,
  same-key follower draining, and shutdown release.
- `JobContext` now contains only `signal` and `log(message)`.
- A successful handler return completes the job. An `Error` retries or fails at
  `maxAttempts`. If a pending follower covers a failed run, Durabl marks the old
  run `superseded`, drains the follower, then throws the original error.
- `FatalJobError` fails the job immediately and accepts `ErrorOptions`.
- `ImmediateBackend` now registers handlers through `queue.process()`.
  `enqueue()` waits for inline processing to finish.
- Direct terminal methods now return explicit result objects.
- `vitest` 2 moved to 4, and all npm advisories are clear. An `overrides` entry
  pins a patched `esbuild`.
- CI actions no longer use the deprecated Node 20 runtime.
- `ImmediateBackend` continues to ignore `delay` because it runs on enqueue.

### Fixed

- Per-claim terminal receipts survive later claims and reconcile the exact
  result of an ambiguous driver write, including `superseded`.
- Bounded shutdown fences new managed runs before its final snapshot. It aborts
  and releases claims that exceed the grace period. A queued follower
  supersedes the old claim.
- Managed heartbeat failures now have a local deadline. Durabl aborts the run
  and suppresses terminal writes when no renewal succeeds within one visibility
  timeout.
- `claimNext` skips a candidate blocked by `dedupeScope: 'pending'` instead of
  surfacing `E11000` and backing off the whole processor type.
- A retried or reaped active job no longer remains wedged behind its pending
  follower. The backend marks the old job `superseded`.
- `DummyBackend.claimNext` now enforces dedupe exclusivity and honors `runAt`.
- All backends now use the shared `assertStepMatches` divergence check.

### BREAKING

- Handlers must return for success, throw `Error` for retry, or throw
  `FatalJobError` for terminal failure. Manual `JobContext` lifecycle methods
  no longer exist.
- `ImmediateBackend.registerHandler` is removed. Register all handlers through
  `queue.process()`.
- `QueueStats.superseded`, `oldestPendingRunAt`, and `oldestPendingLagMs` are
  required fields for every backend.
- `startReaper()` now returns `Promise<StartReaperResult>`. Await the immediate
  recovery result.
- Custom backends must implement `claimNextByKey()`, `release()`, and
  `hasOutstanding()`.
- Custom backend `complete()`, `fail()`, `failFatal()`, and `release()` methods
  must return the new object result unions.

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

[unreleased]: https://github.com/hexsprite/durabl/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/hexsprite/durabl/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/hexsprite/durabl/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/hexsprite/durabl/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/hexsprite/durabl/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/hexsprite/durabl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/hexsprite/durabl/releases/tag/v0.1.0
