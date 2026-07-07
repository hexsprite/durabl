/**
 * Orchestrator integration tests on the in-memory DummyBackend (no Mongo).
 *
 * DummyBackend retries a failed job immediately (no backoff delay), so a thrown
 * error simulates a crash and the next claim is the resume — the journal lives
 * on the same in-memory job across attempts, exactly mirroring durable resume.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { DummyBackend } from '../src/backends/DummyBackend'
import { ImmediateBackend } from '../src/backends/ImmediateBackend'
import { JobQueue } from '../src/JobQueue'
import {
  MaxDurationExceeded,
  OrchestrationUnsupportedError,
} from '../src/journal/errors'
import { LeaseLostError, startHeartbeat } from '../src/orchestrator/context'
import {
  deriveHeartbeatIntervalMs,
  Orchestrator,
} from '../src/orchestrator/Orchestrator'
import { silentLogger } from './testLogger'
import { waitUntil } from './waitUntil'

let backend: DummyBackend
let queue: JobQueue
let orch: Orchestrator

beforeEach(() => {
  backend = new DummyBackend()
  queue = new JobQueue(backend, silentLogger)
  orch = new Orchestrator(queue, silentLogger)
})

afterEach(async () => {
  await queue.shutdown(1000)
})

const statusOf = (id: string): string | undefined =>
  backend.jobs.find((j) => j.id === id)?.status

const fast = { pollInterval: 50 }

describe('Orchestrator construction', () => {
  it('throws if the backend is not journal-capable', () => {
    const bare = { readSteps: undefined } as unknown as never
    const q = new JobQueue(bare, silentLogger)
    expect(() => new Orchestrator(q, silentLogger)).toThrow(
      OrchestrationUnsupportedError,
    )
  })

  it('define() then process() same type is a conflict', () => {
    orch.define('dup', async () => {}, fast)
    expect(() => queue.process('dup', async () => {})).toThrow()
  })

  // Regression: ImmediateBackend runs its own handler registry inline on
  // enqueue and never drives queue.process() processors, so an orchestration
  // wrapper (registered via process) would never execute — the job would sit
  // 'active' forever. Construction must fail loud instead of silently hanging.
  it('rejects an inline-execution backend instead of silently hanging', () => {
    const q = new JobQueue(new ImmediateBackend(), silentLogger)
    expect(() => new Orchestrator(q, silentLogger)).toThrow(
      OrchestrationUnsupportedError,
    )
    expect(() => new Orchestrator(q, silentLogger)).toThrow(/inline/)
  })
})

describe('config validation (du-e1s)', () => {
  // Regression: resolveConfig only rejected an override LOOSER than the
  // reaper window. visibilityTimeoutMs: 0 slipped through, sizing the
  // heartbeat to setTimeout(0) (a per-tick write hammer) and defaulting
  // stepTimeoutMs to 0 (step timeouts silently disabled).
  it('define() rejects visibilityTimeoutMs: 0 instead of hammering heartbeats', () => {
    expect(() =>
      orch.define('v-zero', async () => {}, { visibilityTimeoutMs: 0 }),
    ).toThrow(/visibilityTimeoutMs/)
  })

  it('define() rejects a negative visibilityTimeoutMs', () => {
    expect(() =>
      orch.define('v-neg', async () => {}, { visibilityTimeoutMs: -1 }),
    ).toThrow(/visibilityTimeoutMs/)
  })

  // Regression: NaN failed the `> reaperMs` comparison too, so it passed the
  // old guard and poisoned every derived interval.
  it('define() rejects a NaN visibilityTimeoutMs', () => {
    expect(() =>
      orch.define('v-nan', async () => {}, { visibilityTimeoutMs: NaN }),
    ).toThrow(/visibilityTimeoutMs/)
  })

  // Regression: stepTimeoutMs: 0 silently disabled the per-step liveness cap
  // (withTimeout treats a falsy budget as "no timeout").
  it('define() rejects stepTimeoutMs: 0 instead of silently disabling step timeouts', () => {
    expect(() =>
      orch.define('st-zero', async () => {}, { stepTimeoutMs: 0 }),
    ).toThrow(/stepTimeoutMs/)
  })

  it('define() rejects non-positive heartbeatIntervalMs / maxDurationMs / pollInterval overrides', () => {
    expect(() =>
      orch.define('hb-zero', async () => {}, { heartbeatIntervalMs: 0 }),
    ).toThrow(/heartbeatIntervalMs/)
    expect(() =>
      orch.define('md-neg', async () => {}, { maxDurationMs: -5 }),
    ).toThrow(/maxDurationMs/)
    expect(() =>
      orch.define('pi-zero', async () => {}, { pollInterval: 0 }),
    ).toThrow(/pollInterval/)
  })

  // Regression: the derived heartbeat interval was Math.floor(vt / 3), which is
  // 0 for a 1-2ms visibility window (that window still passes the positive-
  // finite guard) — sizing the heartbeat to setTimeout(0), a per-tick write
  // hammer. The derivation now floors to >= 1ms.
  it('derives a heartbeat cadence of at least 1ms for a tiny visibility window', () => {
    expect(deriveHeartbeatIntervalMs(1)).toBe(1)
    expect(deriveHeartbeatIntervalMs(2)).toBe(1) // floor(2/3) = 0 → clamped to 1
    expect(deriveHeartbeatIntervalMs(3)).toBe(1)
    expect(deriveHeartbeatIntervalMs(300000)).toBe(100000) // normal case intact
  })

  it('define() still accepts positive overrides tighter than the reaper window', () => {
    expect(() =>
      orch.define('ok', async () => {}, {
        ...fast,
        visibilityTimeoutMs: 1000,
        stepTimeoutMs: 500,
        heartbeatIntervalMs: 100,
        maxDurationMs: 2000,
      }),
    ).not.toThrow()
  })
})

describe('resume skips completed steps', () => {
  it('journaled steps return cached results and do not re-run', async () => {
    const effects = { a: 0, b: 0 }
    let crashed = false
    orch.define(
      'flow',
      async (_job, octx) => {
        await octx.step('a', async () => {
          effects.a++
          return 'ra'
        })
        if (!crashed) {
          crashed = true
          throw new Error('boom')
        }
        await octx.step('b', async () => {
          effects.b++
          return 'rb'
        })
      },
      fast,
    )

    const id = (await queue.enqueue('flow', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(effects.a).toBe(1) // ran once, journaled, skipped on resume
    expect(effects.b).toBe(1)
  })
})

describe('at-least-once: crash between side-effect and append', () => {
  it('re-runs the step but with an identical default (jobId-scoped) key', async () => {
    const keys: string[] = []
    let effect = 0
    let dropOnce = true

    const origAppend = backend.appendStep.bind(backend)
    backend.appendStep = async (jobId, token, record) => {
      if (record.name === 'charge' && dropOnce) {
        dropOnce = false
        // Side effect already ran; the append commit is "lost" to a crash.
        throw new Error('append crashed')
      }
      return origAppend(jobId, token, record)
    }

    orch.define(
      'pay',
      async (_job, octx) => {
        await octx.step('charge', async (k) => {
          effect++
          keys.push(k.idempotencyKey)
          return 'ok'
        })
      },
      fast,
    )

    const id = (await queue.enqueue('pay', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(effect).toBe(2) // at-least-once: ran twice
    expect(keys[0]).toBe(keys[1]) // same idempotency key absorbs the double-fire
    expect(keys[0]).toBe(`${id}:0:charge`) // jobId-scoped default
  })

  it('honors an explicit entity-scoped idempotency key across the re-run', async () => {
    const keys: string[] = []
    let dropOnce = true
    const origAppend = backend.appendStep.bind(backend)
    backend.appendStep = async (jobId, token, record) => {
      if (record.name === 'ensure' && dropOnce) {
        dropOnce = false
        throw new Error('append crashed')
      }
      return origAppend(jobId, token, record)
    }

    orch.define(
      'cust',
      async (_job, octx) => {
        await octx.step(
          'ensure',
          async (k) => {
            keys.push(k.idempotencyKey)
            return 'c1'
          },
          { idempotencyKey: 'user42:customer' },
        )
      },
      fast,
    )

    const id = (await queue.enqueue('cust', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(keys).toEqual(['user42:customer', 'user42:customer'])
  })
})

describe('divergence detection', () => {
  it('a changed step name at a journaled seq fails fatally with no retry', async () => {
    let attempt = 0
    orch.define(
      'div',
      async (_job, octx) => {
        attempt++
        const name = attempt === 1 ? 'first' : 'second'
        await octx.step(name, async () => 'x')
        if (attempt === 1) throw new Error('crash')
      },
      fast,
    )

    const id = (await queue.enqueue('div', {})) as string
    await waitUntil(() => statusOf(id) === 'failed')

    const job = backend.jobs.find((j) => j.id === id)
    expect(job?.logs.some((l) => /nondeterminism/i.test(l))).toBe(true)
    expect(attempt).toBe(2) // failed fatally on the 2nd run, no further retries
  })
})

describe('failure semantics', () => {
  it('NonRetryable throws fail fatally with no retry', async () => {
    let runs = 0
    orch.define(
      'nr',
      async () => {
        runs++
        const { NonRetryable } = await import('../src/journal/errors')
        throw new NonRetryable('nope')
      },
      fast,
    )

    const id = (await queue.enqueue('nr', {})) as string
    await waitUntil(() => statusOf(id) === 'failed')
    expect(runs).toBe(1)
  })

  it('a serialization-guard violation fails fatally and names the step', async () => {
    orch.define(
      'bad',
      async (_job, octx) => {
        await octx.step('produce', async () => {
          class Thing {
            x = 1
          }
          return new Thing() as unknown as Record<string, number>
        })
      },
      fast,
    )

    const id = (await queue.enqueue('bad', {})) as string
    await waitUntil(() => statusOf(id) === 'failed')
    const job = backend.jobs.find((j) => j.id === id)
    expect(job?.logs.some((l) => /produce/.test(l))).toBe(true)
  })
})

describe('void steps', () => {
  it('journals completion, skips on resume, replays undefined', async () => {
    let ran = 0
    let observed: unknown = 'unset'
    let crashed = false
    orch.define(
      'void',
      async (_job, octx) => {
        const r = await octx.step('v', async () => {
          ran++
        })
        observed = r
        if (!crashed) {
          crashed = true
          throw new Error('crash')
        }
      },
      fast,
    )

    const id = (await queue.enqueue('void', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(ran).toBe(1) // skipped on resume
    expect(observed).toBeUndefined() // replayed as undefined
  })
})

describe('now() / uuid() determinism', () => {
  it('are constant across resume, distinct per label, and never journaled', async () => {
    const nows: number[] = []
    const uuidA: string[] = []
    const uuidB: string[] = []
    const valueTypes: string[] = []
    let crashed = false

    orch.define(
      'det',
      async (_job, octx) => {
        nows.push(octx.now())
        uuidA.push(octx.uuid('a'))
        uuidB.push(octx.uuid('b'))
        valueTypes.push(typeof octx.now(), typeof octx.uuid('a'))
        await octx.step('s', async () => 'x')
        if (!crashed) {
          crashed = true
          throw new Error('crash')
        }
      },
      fast,
    )

    const id = (await queue.enqueue('det', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(nows[0]).toBe(nows[1]) // constant across resume
    expect(uuidA[0]).toBe(uuidA[1])
    expect(uuidA[0]).not.toBe(uuidB[0])
    expect(valueTypes).toEqual(['number', 'string', 'number', 'string'])
    // One reserved '$bootstrap' record plus the single 's' step — no per-call
    // now/uuid records regardless of how many times they were invoked.
    const steps = await backend.readSteps(id)
    expect(steps.map((s) => s.name)).toEqual(['$bootstrap', 's'])
  })

  // Regression (du-2ap): deriveNow returned job.createdAt — arbitrarily stale
  // for a job that sits pending (scheduled/backed-off) before its first claim.
  // The spec's bootstrap record (§4.1/D1) freezes now() at the wall clock of
  // the FIRST attempt's first use instead.
  it("now() reflects the first attempt's start, not the enqueue time, when execution is delayed", async () => {
    const nows: number[] = []
    let crashed = false

    // Enqueue BEFORE define so no poller can claim the job until we have
    // backdated its createdAt (simulating a long-scheduled/backed-off job).
    const id = (await queue.enqueue('delayed', {})) as string
    const stored = backend.jobs.find((j) => j.id === id)!
    stored.createdAt = new Date(Date.now() - 1_000_000)

    const beforeClaim = Date.now()
    orch.define(
      'delayed',
      async (_job, octx) => {
        nows.push(octx.now())
        await octx.step('s', async () => 'x')
        if (!crashed) {
          crashed = true
          throw new Error('crash')
        }
      },
      fast,
    )
    await waitUntil(() => statusOf(id) === 'completed')

    expect(nows[0]).toBe(nows[1]) // frozen across the simulated crash/resume
    expect(nows[0]).not.toBe(stored.createdAt.getTime()) // NOT the stale enqueue time
    expect(nows[0]).toBeGreaterThanOrEqual(beforeClaim) // wall clock at first use
  })

  // Regression (du-2ap): uuid must derive from the journaled bootstrap seed,
  // not from ephemeral per-attempt state — a resume that re-minted the seed
  // would hand the body different "stable" ids after a crash.
  it('uuid(label) is stable across a simulated crash/resume via the journaled seed', async () => {
    const uuids: string[] = []
    let crashed = false
    orch.define(
      'uuid-stable',
      async (_job, octx) => {
        uuids.push(octx.uuid('customer'))
        await octx.step('s', async () => 'x')
        if (!crashed) {
          crashed = true
          throw new Error('crash')
        }
      },
      fast,
    )

    const id = (await queue.enqueue('uuid-stable', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(uuids).toHaveLength(2)
    expect(uuids[0]).toBe(uuids[1]) // seed read back from the journal on resume
  })

  // The bootstrap record must never trip divergence detection: a resumed body
  // that calls now() again re-reads the seq -1 record instead of re-appending
  // or colliding with user step seqs (which start at 0).
  it('the $bootstrap record does not collide with user steps or trigger NondeterminismError on resume', async () => {
    let crashed = false
    orch.define(
      'boot-div',
      async (_job, octx) => {
        octx.now()
        await octx.step('a', async () => 1)
        await octx.step('b', async () => 2)
        if (!crashed) {
          crashed = true
          throw new Error('crash')
        }
      },
      fast,
    )

    const id = (await queue.enqueue('boot-div', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    const steps = await backend.readSteps(id)
    expect(steps.map((s) => [s.seq, s.name])).toEqual([
      [-1, '$bootstrap'],
      [0, 'a'],
      [1, 'b'],
    ])
  })
})

describe('fan-out (Promise.all over steps)', () => {
  it('assigns stable distinct seqs and resumes correctly', async () => {
    const effects: number[] = []
    const keys: string[] = []
    let crashed = false

    orch.define(
      'fan',
      async (_job, octx) => {
        await Promise.all(
          [0, 1, 2].map((i) =>
            octx.step(`s${i}`, async (k) => {
              effects.push(i)
              keys.push(k.idempotencyKey)
              return i
            }),
          ),
        )
        if (!crashed) {
          crashed = true
          throw new Error('crash')
        }
      },
      fast,
    )

    const id = (await queue.enqueue('fan', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(effects).toHaveLength(3) // all ran once; none re-ran on resume
    expect(new Set(keys).size).toBe(3) // per-iteration keys are distinct
    const steps = await backend.readSteps(id)
    expect(steps.map((s) => s.seq)).toEqual([0, 1, 2])
  })
})

describe('lease fencing on failure paths (du-4ft)', () => {
  // Regression: a claimed job with no claimToken threw a plain Error, which is
  // retryable — every attempt hit the same missing-token wall and burned the
  // whole retry budget. It's a backend-capability defect: fail fatally on the
  // FIRST attempt.
  it('a claimed job missing its claimToken fails fatally on the first attempt', async () => {
    let bodyRuns = 0
    const origClaim = backend.claimNext.bind(backend)
    backend.claimNext = async <T>(type: string) => {
      const job = await origClaim<T>(type)
      // Simulate a backend that never mints claim tokens.
      if (job) job.claimToken = undefined
      return job
    }

    orch.define(
      'tokenless',
      async () => {
        bodyRuns++
      },
      fast,
    )

    const id = (await queue.enqueue('tokenless', {})) as string
    await waitUntil(() => statusOf(id) === 'failed')

    const job = backend.jobs.find((j) => j.id === id)!
    expect(job.attempt).toBe(1) // first attempt, not a burned retry budget
    expect(bodyRuns).toBe(0) // orchestrator body never ran
    expect(job.logs.some((l) => /^Fatal:/.test(l))).toBe(true)
    expect(
      job.logs.some((l) => /durable orchestration.*claimToken/.test(l)),
    ).toBe(true)
  })

  // Regression: the reaper marks an attempt-exhausted job status:'failed'
  // WITHOUT clearing claimToken. completeClaimed matched status:'active' only,
  // so a worker that then finished every step got 'lease-lost' and the
  // completed work was permanently lost.
  it('a run the reaper marked failed (attempt-exhausted) but that finishes all steps ends completed', async () => {
    orch.define(
      'exhausted',
      async (job, octx) => {
        await octx.step('work', async () => 'done')
        // Simulate the reaper firing between the last step and completion:
        // attempt-exhausted → terminal failed, claimToken left in place.
        const stored = backend.jobs.find((j) => j.id === job.id)!
        stored.status = 'failed'
      },
      fast,
    )

    const id = (await queue.enqueue('exhausted', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(statusOf(id)).toBe('completed') // work not lost to the reaper race
  })
})

describe('lease-loss surfacing and abortable duration cap (du-04b)', () => {
  // Regression: startHeartbeat discarded heartbeatClaimed's return, so a
  // 'lease-lost' tick resolved normally and re-armed forever — the orphaned
  // body kept firing side-effecting steps on a job another worker now owned.
  it('a heartbeat tick detecting lease loss stops beating and the next step throws LeaseLostError without running its fn', async () => {
    let hbCalls = 0
    const origHb = backend.heartbeatClaimed.bind(backend)
    backend.heartbeatClaimed = async (jobId, token) => {
      hbCalls++
      return origHb(jobId, token)
    }

    let secondRan = false
    let caught: unknown = null
    orch.define(
      'reclaimed',
      async (job, octx) => {
        await octx.step('first', async () => 'a')
        // Simulate another worker reclaiming the job (new claim token).
        backend.jobs.find((j) => j.id === job.id)!.claimToken = 'stolen'
        // Wait for the auto-heartbeat to notice and abort the run.
        await new Promise<void>((resolve) =>
          octx.signal.addEventListener('abort', () => resolve(), {
            once: true,
          }),
        )
        try {
          await octx.step('second', async () => {
            secondRan = true
          })
        } catch (e) {
          caught = e
          throw e
        }
      },
      { ...fast, heartbeatIntervalMs: 15 },
    )

    await queue.enqueue('reclaimed', {})
    await waitUntil(() => caught !== null)

    expect(caught).toBeInstanceOf(LeaseLostError)
    expect(secondRan).toBe(false) // stopped at the step boundary, no side effect
    const beatsAtStop = hbCalls
    await new Promise((r) => setTimeout(r, 80)) // > 3 intervals
    expect(hbCalls).toBe(beatsAtStop) // loop stopped for good after lease loss
  })

  // Regression: octx.heartbeat() discarded heartbeatClaimed's status — a body
  // manually heartbeating never learned its job had been reclaimed and carried
  // on executing steps.
  it('octx.heartbeat() returns lease-lost after reclaim and the next step throws before executing', async () => {
    let hbResult: unknown = null
    let ran = false
    let caught: unknown = null
    orch.define(
      'manual-hb',
      async (job, octx) => {
        backend.jobs.find((j) => j.id === job.id)!.claimToken = 'stolen'
        hbResult = await octx.heartbeat()
        try {
          await octx.step('after', async () => {
            ran = true
          })
        } catch (e) {
          caught = e
          throw e
        }
      },
      fast,
    )

    await queue.enqueue('manual-hb', {})
    await waitUntil(() => caught !== null)

    expect(hbResult).toBe('lease-lost')
    expect(caught).toBeInstanceOf(LeaseLostError)
    expect(ran).toBe(false) // fn never fired
  })

  // Regression: the maxDurationMs Promise.race rejected but never cancelled
  // the body — the orphaned run raced on and fired later steps' side effects
  // after the job had already been failed.
  it('a maxDurationMs breach prevents subsequent steps of the orphaned body from running their fn', async () => {
    let ranTwo = false
    let caught: unknown = null
    orch.define(
      'capped',
      async (_job, octx) => {
        await octx.step('one', async () => 'x')
        // Outlive the 40ms cap (test-only sleep; real bodies only await octx.*).
        await new Promise((r) => setTimeout(r, 120))
        try {
          await octx.step('two', async () => {
            ranTwo = true
          })
        } catch (e) {
          caught = e
          throw e
        }
      },
      { ...fast, maxDurationMs: 40 },
    )

    const id = (await queue.enqueue('capped', {}, { maxAttempts: 1 })) as string
    await waitUntil(() => caught !== null)

    expect(caught).toBeInstanceOf(MaxDurationExceeded)
    expect(ranTwo).toBe(false) // orphan stopped at the step boundary
    expect(statusOf(id)).toBe('failed') // attempt budget exhausted → failed
  })
})

describe('startHeartbeat stop/complete race', () => {
  // Regression: on successful completion the wrapper flips the status to
  // 'completed'. A heartbeat tick already in flight then read 'lease-lost' and
  // fired onLeaseLost — a spurious 'lease lost — aborting run' warn + run abort
  // on an already-finished run. A tick that resolves after stop() must be a
  // no-op.
  it('a tick resolving after stop() does not fire onLeaseLost', async () => {
    let resolveHb!: (r: 'heartbeated' | 'lease-lost') => void
    const hbInFlight = new Promise<'heartbeated' | 'lease-lost'>((res) => {
      resolveHb = res
    })
    let hbCalled = false
    const fakeQueue = {
      heartbeatClaimed: () => {
        hbCalled = true
        return hbInFlight
      },
    } as unknown as JobQueue

    let leaseLost = false
    let warned = false
    const handle = startHeartbeat(
      fakeQueue,
      'job1',
      'tok',
      1,
      () => {
        warned = true
      },
      () => {
        leaseLost = true
      },
    )

    // Let the first tick fire and suspend on the in-flight heartbeat write.
    await waitUntil(() => hbCalled)
    // The run completed and stopped the heartbeat while the write was pending.
    handle.stop()
    // Now the write comes back reporting the reclaim the completion caused.
    resolveHb('lease-lost')
    await new Promise((r) => setTimeout(r, 20))

    expect(leaseLost).toBe(false) // ignored: stopped before it resolved
    expect(warned).toBe(false)
  })
})

describe('duplicate-append ambiguity', () => {
  it('returns the stored result when appendStep reports already-recorded', async () => {
    let ran = 0
    let observed: unknown = null
    let once = true
    const origAppend = backend.appendStep.bind(backend)
    backend.appendStep = async (jobId, token, record) => {
      const res = await origAppend(jobId, token, record) // actually appends
      if (record.name === 'dup' && once) {
        once = false
        // Pretend the ack was lost after the write: report already-recorded
        // carrying the record that landed, as a driver-retry re-read would.
        return { status: 'already-recorded', existing: record }
      }
      return res
    }

    orch.define(
      'amb',
      async (_job, octx) => {
        observed = await octx.step('dup', async () => {
          ran++
          return 'val'
        })
      },
      fast,
    )

    const id = (await queue.enqueue('amb', {})) as string
    await waitUntil(() => statusOf(id) === 'completed')

    expect(ran).toBe(1)
    expect(observed).toBe('val')
    const steps = await backend.readSteps(id)
    expect(steps).toHaveLength(1) // no duplicate row
  })
})
