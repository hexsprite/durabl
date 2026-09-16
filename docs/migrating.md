# Migration guides

Step-by-step upgrade instructions for releases that break something. Each
release's full list of changes lives in [`CHANGELOG.md`](../CHANGELOG.md); this
file holds the long-form migrations that do not fit there.

While the version is `0.x`, breaking changes ship in a minor release.

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

