/**
 * Shared polling helper for the test suites: wait until `predicate` (sync or
 * async) returns truthy, or throw after `timeoutMs`.
 */
export interface WaitUntilOptions {
  /** Give up (and throw) after this long. Default: 10000. */
  timeoutMs?: number
  /** Delay between predicate checks. Default: 10. */
  pollMs?: number
}

export async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  options: number | WaitUntilOptions = {},
): Promise<void> {
  const opts = typeof options === 'number' ? { timeoutMs: options } : options
  const { timeoutMs = 10000, pollMs = 10 } = opts
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return
    await new Promise((r) => setTimeout(r, pollMs))
  }
  throw new Error('waitUntil timed out')
}
