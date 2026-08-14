# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x`, breaking changes ship in minor releases and are
called out under **BREAKING**.

## [0.2.2] - 2026-08-14

### Added

- Durabl backends return a lease-aware inline handle whose `heartbeat()` method
  extends long-running leases. The result is fenced, so `'lease-lost'` tells
  the caller to stop after another worker reclaims the job. The base
  `JobHandle` keeps this method optional for 0.2.x custom-backend compatibility;
  generic `JobQueue` typing preserves the capability supplied by its backend.

### Fixed

- `claimNext()` skips any number of pending jobs whose dedupe key still has an
  active owner. Blocked keys no longer throw `E11000`, back off every processor
  for that job type, or starve claimable jobs later in the queue.
- Failed or reaped active jobs no longer wedge when a pending follow-up already
  owns the pending dedupe slot. The follow-up inherits the consumed attempt and
  retry delay before the active job retires as coalesced. A fenced retirement
  lock prevents lease renewal from racing that transfer.
- The reaper fences recovery writes against the lease snapshot, so a heartbeat
  that lands after the stale scan prevents recovery.
- `DummyBackend.claimNext()` now models Mongo's pending-scope active-key block.

## [0.2.1] - 2026-08-07

### Added

- `durabl/testing`, a crash/resume harness for exercising orchestrator bodies
  without hand-written failure injection.

## [0.2.0] - 2026-08-06

### Added

- Durable orchestration with step-level resume and lease-fenced journal writes.
- `durabl/eslint` with `no-nondeterministic-control-path`.

### Fixed

- Dedupe, lease, shutdown, log, reaper, and retry-backoff hardening.

## [0.1.1] - 2026-06-12

### Changed

- Ship CommonJS first so Meteor and other CommonJS bundlers load the package.

## [0.1.0] - 2026-06-10

### Added

- Initial Mongo-backed durable job queue.

[0.2.2]: https://github.com/hexsprite/durabl/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/hexsprite/durabl/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/hexsprite/durabl/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/hexsprite/durabl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/hexsprite/durabl/releases/tag/v0.1.0
