/**
 * Retry backoff calculation.
 *
 * Shared by the normal retry path (`fail()`) and stalled-job recovery
 * (`recoverStuckJobs()`) so both space retries the same way. Without a delay a
 * fast-failing handler exhausts all attempts in milliseconds, and a downstream
 * outage turns every retry into an instant-retry storm against the dead
 * dependency. Spacing — with jitter — lets transients clear and spreads reload.
 */
import type { BackoffConfig } from '../types'

const DEFAULT_BASE_MS = 1000
const DEFAULT_MAX_MS = 60000

/**
 * Delay (ms) before a job's attempt N becomes claimable again.
 *
 * - `'fixed'` — constant `baseDelayMs` (capped at `maxDelayMs`).
 * - `'exponential'` (default) — exponential with **full jitter**, floored at
 *   `baseDelayMs`: `max(base, random() * min(cap, base * 2^(attempt-1)))`.
 *   Full jitter (vs. fixed exponential) is what de-synchronizes a herd of jobs
 *   that all failed on the same tick.
 *
 * @param attempt 1-based number of the attempt that just failed.
 * @param cfg     Per-job backoff config (persisted on the job doc).
 */
export function retryBackoffMs(attempt: number, cfg: BackoffConfig = {}): number {
  const base = cfg.backoffDelay ?? DEFAULT_BASE_MS
  const cap = cfg.backoffMaxDelay ?? DEFAULT_MAX_MS

  if (cfg.backoff === 'fixed') {
    return Math.min(cap, base)
  }

  const exp = Math.min(cap, base * 2 ** Math.max(0, attempt - 1))
  return Math.max(base, Math.random() * exp)
}
