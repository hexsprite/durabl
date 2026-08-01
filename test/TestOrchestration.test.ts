/**
 * durabl/testing harness (du-hmw): orchestrator bodies unit-tested against the
 * real step machine over an in-memory journal — no queue, no backend.
 */
import { describe, expect, it, vi } from 'vitest'

import { NondeterminismError, NonSerializableStepResult } from '../src'
import type { OrchestratorContext } from '../src'
import {
  SimulatedCrash,
  TEST_SEED,
  TEST_STARTED_AT,
  testOrchestration,
} from '../src/testing'
import { deriveUuid } from '../src/orchestrator/context'

interface Payload {
  userId: string
}

describe('testOrchestration', () => {
  it('runs a body to completion and records the steps in order', async () => {
    const t = testOrchestration<Payload>(
      async (job, octx) => {
        const a = await octx.step('load', async () => `sub-for-${job.data.userId}`)
        await octx.step('save', async () => ({ loaded: a }))
      },
      { data: { userId: 'u1' } },
    )

    await t.run()

    expect(t.steps).toEqual([
      { seq: 0, name: 'load', result: 'sub-for-u1' },
      { seq: 1, name: 'save', result: { loaded: 'sub-for-u1' } },
    ])
    expect(t.attempts).toBe(1)
  })

  it('crashAfter journals the step; resume skips it (side effect fires once)', async () => {
    // Symptom this harness exists for: asserting Stripe-style calls do NOT
    // double-fire across a crash/resume of the same job.
    const createSub = vi.fn(async () => 'sub_123')
    const sync = vi.fn(async () => undefined)
    const body = async (_job: unknown, octx: OrchestratorContext): Promise<void> => {
      await octx.step('create-sub', createSub)
      await octx.step('sync', sync)
    }

    const t = testOrchestration(body)
    await t.crashAfter('create-sub')

    expect(createSub).toHaveBeenCalledTimes(1)
    expect(sync).not.toHaveBeenCalled()
    expect(t.steps.map((s) => s.name)).toEqual(['create-sub'])

    await t.resume()

    expect(createSub).toHaveBeenCalledTimes(1) // replayed, not re-run
    expect(sync).toHaveBeenCalledTimes(1)
    expect(t.attempts).toBe(2)
  })

  it('crashBefore loses the journal write; resume re-runs the step', async () => {
    // The driver-retry window per-step idempotency keys defend against: the
    // side effect fired but the append never landed.
    const createSub = vi.fn(async () => 'sub_123')
    const t = testOrchestration(async (_job, octx) => {
      await octx.step('create-sub', createSub)
    })

    await t.crashBefore('create-sub')
    expect(createSub).toHaveBeenCalledTimes(1)
    expect(t.steps).toEqual([]) // nothing journaled

    await t.resume()
    expect(createSub).toHaveBeenCalledTimes(2) // re-ran — key must dedupe
  })

  it('passes a stable jobId-scoped idempotency key into the step on re-run', async () => {
    const seen: string[] = []
    const body = async (_job: unknown, octx: OrchestratorContext): Promise<void> => {
      await octx.step('create-sub', async (keys) => {
        seen.push(keys.idempotencyKey)
        return 'ok'
      })
    }
    const t = testOrchestration(body, { jobId: 'job-9' })

    await t.crashBefore('create-sub')
    await t.resume()

    expect(seen).toEqual(['job-9:0:create-sub', 'job-9:0:create-sub'])
  })

  it('now() and uuid() are deterministic and stable across resume', async () => {
    const observed: Array<{ now: number; id: string }> = []
    const body = async (_job: unknown, octx: OrchestratorContext): Promise<void> => {
      observed.push({ now: octx.now(), id: octx.uuid('invoice') })
      await octx.step('after-bootstrap', async () => 'x')
    }

    const t = testOrchestration(body)
    await t.crashAfter('after-bootstrap')
    await t.resume()

    expect(observed).toHaveLength(2)
    expect(observed[0]).toEqual(observed[1])
    expect(observed[0].now).toBe(TEST_STARTED_AT)
    expect(observed[0].id).toBe(deriveUuid(TEST_SEED, 'invoice'))
  })

  it('surfaces NondeterminismError when the resumed body diverges', async () => {
    // Deploy-hazard regression: a body edit that renames/reorders steps must
    // fail the unit test, not strand the job in production.
    const v1 = async (_job: unknown, octx: OrchestratorContext): Promise<void> => {
      await octx.step('step-a', async () => 1)
      await octx.step('step-b', async () => 2)
    }
    const t = testOrchestration(v1)
    await t.crashAfter('step-a')

    // "Deploy" a body whose first step has a different name.
    const v2 = async (_job: unknown, octx: OrchestratorContext): Promise<void> => {
      await octx.step('step-renamed', async () => 1)
    }
    await expect(t.resumeWith(v2)).rejects.toThrow(NondeterminismError)
  })

  it('rejects non-serializable step results, same as production backends', async () => {
    const t = testOrchestration(async (_job, octx) => {
      await octx.step('bad', async () => ({ fn: () => 1 }))
    })
    await expect(t.run()).rejects.toThrow(NonSerializableStepResult)
  })

  it('collects octx.log lines across attempts', async () => {
    const t = testOrchestration(async (_job, octx) => {
      octx.log('starting')
      await octx.step('s', async () => 'ok')
    })
    await t.crashAfter('s')
    await t.resume()
    expect(t.logs).toEqual(['starting', 'starting'])
  })

  it('throws when the named crash step never runs', async () => {
    const t = testOrchestration(async (_job, octx) => {
      await octx.step('real-step', async () => 'ok')
    })
    await expect(t.crashAfter('no-such-step')).rejects.toThrow(
      /expected a crash at step 'no-such-step'/,
    )
  })

  it('exports SimulatedCrash for custom catch assertions', () => {
    expect(new SimulatedCrash('x', 'after')).toBeInstanceOf(Error)
  })
})
