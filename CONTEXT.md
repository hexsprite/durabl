# Durabl Queue

Durabl provides durable, single-flight job execution across interchangeable storage backends.

## Language

**Active run**:
A job that one worker currently owns through a lease and claim token.
_Avoid_: Lock, active task

**Pending follower**:
The one pending job queued behind an active run for the same type and dedupe key. It represents requests received while the active run executes.
_Avoid_: Request record, latest-value record

**Replaceable workload**:
A state-snapshot or recomputation workload whose intermediate payloads can be discarded without losing required work.
_Avoid_: Latest job, coalesced command

**Superseded run**:
A terminal active run whose pending follower already preserves newer work. It is not a failed run and cannot retry.
_Avoid_: Failed duplicate, dropped job

**Managed execution**:
Queue-owned execution that controls the lease and derives the terminal transition from the callback outcome.
_Avoid_: Managed handler, automatic mode

**Outstanding work**:
Pending or active work for one type and dedupe key. It is a point-in-time diagnostic state, not a coordination lock.
_Avoid_: Queue lock, authoritative state

**Terminal receipt**:
A per-claim record of one lifecycle operation and its durable result. Receipts survive later claims so an ambiguous write can recover its exact result.
_Avoid_: Completion log, mutable latest-result slot

**Shutdown release**:
The queue action that aborts an overdue managed run and returns its live claim to the due pending queue without consuming an attempt.
_Avoid_: Rollback, shutdown retry
