/**
 * Meta-tests for the chaos/property harness itself.
 *
 * A harness that cannot fail is worse than no harness: it reports green forever
 * and quietly retires the invariants it claims to guard. These tests prove the
 * detector detects and the injector injects, so a green run in
 * queueProperties/queueChaos means something.
 */
import { describe, expect, it } from 'vitest'

import { DummyBackend } from '../../src/backends/DummyBackend'

import {
  type FaultProfile,
  makeFaultyBackend,
  makeRng,
} from './FaultyBackend'
import { checkInvariants, type ModelJob } from './queueModel'

const job = (over: Partial<ModelJob> = {}): ModelJob => ({
  id: 'j1',
  type: 'sync',
  status: 'pending',
  attempt: 0,
  maxAttempts: 3,
  priority: 0,
  delayMs: 0,
  runAtMs: 0,
  executions: 0,
  terminalWrites: 0,
  ...over,
})

describe('checkInvariants actually detects violations', () => {
  it('passes a healthy job set', () => {
    expect(
      checkInvariants([
        job({ id: 'a', status: 'active', attempt: 1, dedupeKey: 'k1' }),
        job({ id: 'b', status: 'pending', dedupeKey: 'k1' }),
      ]),
    ).toEqual([])
  })

  it('catches two active jobs on one key+scope', () => {
    const v = checkInvariants([
      job({ id: 'a', status: 'active', attempt: 1, dedupeKey: 'k1', dedupeScope: 'pending' }),
      job({ id: 'b', status: 'active', attempt: 1, dedupeKey: 'k1', dedupeScope: 'pending' }),
    ])
    expect(v).toHaveLength(1)
    expect(v[0]).toContain('mutual exclusion broken')
  })

  it('allows one key across two different scopes', () => {
    // Enforced by separate partial indexes, so this is legal today. See the
    // bead on whether it should be.
    expect(
      checkInvariants([
        job({ id: 'a', status: 'active', attempt: 1, dedupeKey: 'k1', dedupeScope: 'pending' }),
        job({
          id: 'b',
          status: 'active',
          attempt: 1,
          dedupeKey: 'k1',
          dedupeScope: 'pending+active',
        }),
      ]),
    ).toEqual([])
  })

  it('catches attempt overrunning maxAttempts', () => {
    const v = checkInvariants([job({ attempt: 4, maxAttempts: 3 })])
    expect(v[0]).toContain('exceeds maxAttempts')
  })

  it('catches a double terminal write', () => {
    const v = checkInvariants([job({ terminalWrites: 2 })])
    expect(v[0]).toContain('terminal writes')
  })

  it('catches an active job that was never claimed', () => {
    const v = checkInvariants([job({ status: 'active', attempt: 0 })])
    expect(v[0]).toContain('never claimed')
  })
})

describe('makeRng is deterministic', () => {
  it('produces the same sequence for one seed', () => {
    const a = makeRng(42)
    const b = makeRng(42)
    const seqA = Array.from({ length: 8 }, () => a())
    const seqB = Array.from({ length: 8 }, () => b())
    expect(seqA).toEqual(seqB)
  })

  it('produces different sequences for different seeds', () => {
    const a = makeRng(1)
    const b = makeRng(2)
    expect(a()).not.toBe(b())
  })
})

describe('makeFaultyBackend actually injects', () => {
  const profile: FaultProfile = {
    rate: 1, // every eligible call
    kinds: ['throw'],
    methods: ['enqueue'],
  }

  it('faults an eligible method', async () => {
    const { backend, injected } = makeFaultyBackend(
      new DummyBackend(),
      profile,
      7,
    )
    await expect(backend.enqueue('sync', {})).rejects.toThrow('injected fault')
    expect(injected).toEqual([{ method: 'enqueue', kind: 'throw' }])
  })

  it('leaves ineligible methods alone', async () => {
    const { backend, injected } = makeFaultyBackend(
      new DummyBackend(),
      profile,
      7,
    )
    // claimNext is not in `methods`, so it must pass straight through.
    await expect(backend.claimNext('sync')).resolves.toBeNull()
    expect(injected).toEqual([])
  })

  it('drop resolves without touching the backend', async () => {
    const inner = new DummyBackend()
    const { backend } = makeFaultyBackend(
      inner,
      { rate: 1, kinds: ['drop'], methods: ['enqueue'] },
      3,
    )
    await backend.enqueue('sync', {})
    // The caller was told it succeeded; nothing was written. This is the fault
    // that makes lease fencing worth verifying rather than assuming.
    expect(inner.getJobsByType('sync')).toHaveLength(0)
  })

  it('injects nothing at rate 0', async () => {
    const inner = new DummyBackend()
    const { backend, injected } = makeFaultyBackend(
      inner,
      { rate: 0, kinds: ['throw'], methods: ['enqueue'] },
      5,
    )
    await backend.enqueue('sync', {})
    expect(injected).toEqual([])
    expect(inner.getJobsByType('sync')).toHaveLength(1)
  })
})
