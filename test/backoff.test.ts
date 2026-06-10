import { describe, expect, it } from 'vitest'

import { retryBackoffMs } from '../src/backends/backoff'

describe('retryBackoffMs', () => {
  it('defaults to exponential, floored at 1000ms on the first attempt', () => {
    // attempt 1: exp = base * 2^0 = base; full jitter in [0, base] then floored.
    for (let i = 0; i < 50; i++) {
      expect(retryBackoffMs(1)).toBe(1000)
    }
  })

  it('fixed strategy returns a constant base delay', () => {
    for (let attempt = 1; attempt <= 10; attempt++) {
      expect(retryBackoffMs(attempt, { backoff: 'fixed', backoffDelay: 2500 })).toBe(
        2500,
      )
    }
  })

  it('fixed strategy is capped at backoffMaxDelay', () => {
    expect(
      retryBackoffMs(1, {
        backoff: 'fixed',
        backoffDelay: 100000,
        backoffMaxDelay: 30000,
      }),
    ).toBe(30000)
  })

  it('exponential stays within [base, cap] and never exceeds the cap', () => {
    const cfg = { backoffDelay: 1000, backoffMaxDelay: 8000 }
    for (let attempt = 1; attempt <= 20; attempt++) {
      for (let i = 0; i < 20; i++) {
        const d = retryBackoffMs(attempt, cfg)
        expect(d).toBeGreaterThanOrEqual(1000)
        expect(d).toBeLessThanOrEqual(8000)
      }
    }
  })

  it('exponential ceiling grows with attempt until the cap', () => {
    // The pre-jitter ceiling is min(cap, base * 2^(attempt-1)). With base 1000
    // cap 60000: attempt 1 -> 1000 (exactly base), attempt 7 -> 64000 capped to
    // 60000. Sample the max across many draws to observe the widening window.
    const cfg = { backoffDelay: 1000, backoffMaxDelay: 60000 }
    const maxAt = (attempt: number) => {
      let m = 0
      for (let i = 0; i < 500; i++) m = Math.max(m, retryBackoffMs(attempt, cfg))
      return m
    }
    expect(maxAt(1)).toBe(1000)
    // attempt 4 ceiling = 8000; sampled max should land well above attempt 2's.
    expect(maxAt(4)).toBeGreaterThan(maxAt(2))
  })
})
