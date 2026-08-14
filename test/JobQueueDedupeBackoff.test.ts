/**
 * Regression coverage for the JobQueue half of du-pz9.
 *
 * The bug lived in `MongoJobQueue.claimNext` (it let a dedupe-index E11000
 * escape) but the DAMAGE was done here: `claimAndProcess` catches any throw from
 * `claimNext` and applies exponential backoff to the ProcessorState, which is
 * keyed on job TYPE. So contention on a single dedupeKey escalated the poll
 * delay toward 60s for every key of that type.
 *
 * The backend-level fix is covered in MongoJobQueueDedupeClaim.test.ts. These
 * tests pin the contract at this layer, so that a future backend which surfaces
 * dedupe contention as a throw cannot quietly reintroduce the stall, and so that
 * genuine backend failures still back off.
 */
import { describe, expect, it, vi } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { JobQueue } from '../src/JobQueue'
import type { Job } from '../src/types'

/**
 * Read the private backoff for a processor. Kept in one helper so a rename of
 * `processors`/`backoffMs` fails from a single line rather than several.
 */
function getBackoff(q: JobQueue, type: string): number | undefined {
  return (
    q as unknown as { processors: Map<string, { backoffMs: number }> }
  ).processors.get(type)?.backoffMs
}


const silent = {
  child: () => silent,
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
}

describe('JobQueue: dedupe contention must not trip processor backoff', () => {
  it('stays at zero backoff when claimNext reports nothing claimable', async () => {
    // A correct backend returns null for "everything claimable is blocked".
    const claimNext = vi.fn().mockResolvedValue(null as Job | null)
    const backend = new DummyBackend()
    vi.spyOn(backend, 'claimNext').mockImplementation(claimNext)
    const queue = new JobQueue(backend, silent)

    queue.process('sync', async () => {}, { pollInterval: 10 })
    await new Promise((r) => setTimeout(r, 60))

    expect(getBackoff(queue, 'sync')).toBe(0)
    expect(claimNext).toHaveBeenCalled()
    await queue.shutdown(100)
  })

  it('does back off when claimNext fails for a real reason', async () => {
    // The inverse guarantee: swallowing dedupe contention must not have made
    // the queue blind to genuine backend failure.
    const claimNext = vi
      .fn()
      .mockRejectedValue(new Error('connection reset by peer'))
    const backend = new DummyBackend()
    vi.spyOn(backend, 'claimNext').mockImplementation(claimNext)
    const queue = new JobQueue(backend, silent)

    queue.process('sync', async () => {}, { pollInterval: 10 })
    await new Promise((r) => setTimeout(r, 60))

    expect(getBackoff(queue, 'sync')).toBeGreaterThan(0)
    await queue.shutdown(100)
  })
})
