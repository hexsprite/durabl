/**
 * Managed JobQueue lifecycle fencing (du-4ft): automatic completion and
 * handler failures must use the claimed job's claimToken so a zombie worker
 * (one whose lease was reaped and re-issued to another worker) can never
 * clobber the live owner's job.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { FatalJobError, JobQueue } from '../src'
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

describe('managed JobQueue lifecycle fencing (zombie worker)', () => {
  // Regression (du-4ft): managed failure called backend.fail(job.id, reason)
  // with no claim token. A worker that stalled past the visibility timeout,
  // had its job reclaimed, and *then* threw would fail the job out from under
  // the new owner (resetting it to pending or failed mid-run).
  it("a managed handler that throws after reclaim does not clobber the new owner's job", async () => {
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

  // Regression (du-4ft): automatic completion was unfenced — a zombie
  // returning after reclaim would complete the live owner's in-flight run.
  it("a managed handler returning after reclaim does not complete the new owner's job", async () => {
    const complete = vi.spyOn(backend, 'complete')
    queue.process(
      't',
      async (job) => {
        const stored = backend.jobs.find((j) => j.id === job.id)!
        stored.claimToken = 'new-owner-token'
      },
      fast,
    )
    const id = (await queue.enqueue('t', {})) as string
    await waitUntil(() => complete.mock.calls.length === 1)

    const stored = backend.jobs.find((j) => j.id === id)!
    expect(stored.status).toBe('active')
  })

  // Regression (du-4ft): FatalJobError handling was unfenced — a zombie
  // throwing after reclaim could terminally fail the live owner's run.
  it("a managed handler throwing FatalJobError after reclaim does not fail the new owner's job", async () => {
    const failFatal = vi.spyOn(backend, 'failFatal')
    queue.process(
      't',
      async (job) => {
        const stored = backend.jobs.find((j) => j.id === job.id)!
        stored.claimToken = 'new-owner-token'
        throw new FatalJobError('zombie fatal')
      },
      fast,
    )
    const id = (await queue.enqueue('t', {})) as string
    await waitUntil(() => failFatal.mock.calls.length === 1)

    const stored = backend.jobs.find((j) => j.id === id)!
    expect(stored.status).toBe('active')
    expect(stored.logs).toHaveLength(0)
  })

  it('a live managed handler still completes after returning normally', async () => {
    queue.process(
      'ok',
      async () => {},
      fast,
    )
    const id = (await queue.enqueue('ok', {})) as string
    await waitUntil(
      () => backend.jobs.find((j) => j.id === id)?.status === 'completed',
    )
    expect(backend.jobs.find((j) => j.id === id)!.status).toBe('completed')
  })
})
