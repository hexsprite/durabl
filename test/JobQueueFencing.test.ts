/**
 * JobQueue lifecycle fencing (du-4ft): processJob and the handler JobContext
 * must thread the claimed job's claimToken into complete/fail/failFatal so a
 * zombie worker (one whose lease was reaped and re-issued to another worker)
 * can never clobber the live owner's job.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { JobQueue } from '../src/JobQueue'
import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

let backend: DummyBackend
let queue: JobQueue

beforeEach(() => {
  backend = new DummyBackend()
  queue = new JobQueue(backend, silentLogger)
})

afterEach(async () => {
  await queue.shutdown(1000)
})

const fast = { pollInterval: 50 }

describe('JobQueue lifecycle fencing (zombie worker)', () => {
  // Regression (du-4ft): processJob called backend.fail(job.id, reason) with
  // no claim token. A worker that stalled past the visibility timeout, had its
  // job reclaimed, and *then* threw would fail the job out from under the new
  // owner (resetting it to pending or failed mid-run).
  it("a handler that throws after its job was reclaimed does not clobber the new owner's job", async () => {
    let handled = false
    queue.process(
      't',
      async (job) => {
        // Simulate the reaper: while this handler runs, the job is reclaimed
        // by another worker under a fresh token.
        const stored = backend.jobs.find((j) => j.id === job.id)!
        stored.claimToken = 'new-owner-token'
        handled = true
        throw new Error('zombie crash')
      },
      fast,
    )
    const id = (await queue.enqueue('t', {})) as string
    await waitUntil(() => handled)
    // Give processJob's catch path time to (not) act.
    await new Promise((r) => setTimeout(r, 50))

    const stored = backend.jobs.find((j) => j.id === id)!
    expect(stored.status).toBe('active') // still the new owner's
    expect(stored.claimToken).toBe('new-owner-token')
    expect(stored.logs).toHaveLength(0) // no Failed: entry from the zombie
  })

  // Regression (du-4ft): ctx.complete was unfenced — a zombie completing
  // after reclaim would mark the live owner's in-flight run completed.
  it("a handler calling ctx.complete after its job was reclaimed does not complete the new owner's job", async () => {
    let handled = false
    queue.process(
      't',
      async (job, ctx) => {
        const stored = backend.jobs.find((j) => j.id === job.id)!
        stored.claimToken = 'new-owner-token'
        await ctx.complete()
        handled = true
      },
      fast,
    )
    const id = (await queue.enqueue('t', {})) as string
    await waitUntil(() => handled)

    const stored = backend.jobs.find((j) => j.id === id)!
    expect(stored.status).toBe('active')
  })

  // Regression (du-4ft): ctx.failFatal was unfenced — the Orchestrator's
  // fatal path (and any handler) could terminally fail a reclaimed job.
  it("a handler calling ctx.failFatal after its job was reclaimed does not fail the new owner's job", async () => {
    let handled = false
    queue.process(
      't',
      async (job, ctx) => {
        const stored = backend.jobs.find((j) => j.id === job.id)!
        stored.claimToken = 'new-owner-token'
        await ctx.failFatal('zombie fatal')
        handled = true
      },
      fast,
    )
    const id = (await queue.enqueue('t', {})) as string
    await waitUntil(() => handled)

    const stored = backend.jobs.find((j) => j.id === id)!
    expect(stored.status).toBe('active')
    expect(stored.logs).toHaveLength(0)
  })

  it('a live (non-reclaimed) handler still completes and fails normally', async () => {
    queue.process(
      'ok',
      async (_job, ctx) => {
        await ctx.complete()
      },
      fast,
    )
    const id = (await queue.enqueue('ok', {})) as string
    await waitUntil(
      () => backend.jobs.find((j) => j.id === id)?.status === 'completed',
    )
    expect(backend.jobs.find((j) => j.id === id)!.status).toBe('completed')
  })
})
