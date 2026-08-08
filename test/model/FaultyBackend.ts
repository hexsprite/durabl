/**
 * Fault-injecting decorator over any {@link IJobQueueBackend} (du-btm Part B).
 *
 * The original bead asked for killed workers, network partitions and restarted
 * nodes. None of that is implementable against `mongodb-memory-server`, and
 * aiming at it is why the bead stalled — but it was also aiming at the wrong
 * target. Every failure mode listed has an exact in-process equivalent, because
 * what is under test is durabl's REACTION to a fault, not the fault mechanism:
 *
 * - worker death        = a handler that returns without a terminal write
 * - stalled heartbeat   = a handler that awaits past its visibility timeout
 * - partition / node    = a backend call that throws or hangs
 * - driver retry        = a backend call applied twice
 *
 * So faults are injected at the backend seam instead.
 *
 * Every decision comes from a seeded PRNG. `Math.random()` is forbidden here:
 * a failure that cannot be replayed from its seed produces unactionable red
 * builds, and a harness people learn to ignore is worse than no harness.
 */
import type { IJobQueueBackend } from '../../src/backends/IJobQueueBackend'

export type FaultKind = 'drop' | 'throw' | 'duplicate' | 'delay'

export interface FaultProfile {
  /** Probability (0..1) that a given call is faulted at all. */
  rate: number
  /** Which faults are eligible. Chosen uniformly from this list. */
  kinds: FaultKind[]
  /** Methods eligible for faulting. Others always pass through. */
  methods: string[]
  /** Delay applied by the 'delay' fault, ms. */
  delayMs?: number
}

/**
 * Deterministic PRNG (mulberry32). Small, seedable, and good enough to explore
 * interleavings — this is not cryptography.
 */
export function makeRng(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export interface InjectedFault {
  method: string
  kind: FaultKind
}

/**
 * Wrap a backend so selected calls misbehave.
 *
 * `drop` resolves without touching the backend, which models a write that never
 * landed — the caller believes it succeeded. That is the nastiest of the four
 * and the reason lease fencing has to be checked rather than assumed.
 */
export function makeFaultyBackend<T extends IJobQueueBackend>(
  inner: T,
  profile: FaultProfile,
  seed: number,
): { backend: IJobQueueBackend; injected: InjectedFault[] } {
  const rng = makeRng(seed)
  const injected: InjectedFault[] = []

  const pick = (method: string): FaultKind | null => {
    if (!profile.methods.includes(method)) return null
    if (rng() >= profile.rate) return null
    const kind = profile.kinds[Math.floor(rng() * profile.kinds.length)]
    return kind ?? null
  }

  const handler: ProxyHandler<IJobQueueBackend> = {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function' || typeof prop !== 'string') return value

      return async (...args: unknown[]) => {
        const fault = pick(prop)
        if (fault) injected.push({ method: prop, kind: fault })

        switch (fault) {
          case 'drop':
            // The write never happened, but the caller is told it did.
            return undefined
          case 'throw':
            throw new Error(`injected fault: ${prop} failed`)
          case 'delay':
            await new Promise((r) => setTimeout(r, profile.delayMs ?? 5))
            return (value as (...a: unknown[]) => unknown).apply(target, args)
          case 'duplicate': {
            // Driver-level retry: the same call applied twice. Idempotent
            // operations must survive it; the first result is what the caller
            // would have seen.
            const first = await (
              value as (...a: unknown[]) => Promise<unknown>
            ).apply(target, args)
            await (value as (...a: unknown[]) => Promise<unknown>)
              .apply(target, args)
              .catch(() => {
                /* a rejected retry is itself a normal outcome */
              })
            return first
          }
          default:
            return (value as (...a: unknown[]) => unknown).apply(target, args)
        }
      }
    },
  }

  return { backend: new Proxy(inner, handler), injected }
}
