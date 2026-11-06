/**
 * Simulate a host whose wall clock is off by `skewMs`.
 *
 * Replaces the global `Date` with a subclass whose `now()` and no-arg
 * constructor are shifted. Real timers (`setTimeout`) are untouched, so the
 * Mongo driver and the test runner keep working. Two queue instances in one
 * process act as two hosts by changing the skew between their calls.
 */
const RealDate = Date
let skewMs = 0

class SkewedDate extends RealDate {
  constructor(...args: unknown[]) {
    if (args.length === 0) super(RealDate.now() + skewMs)
    else super(...(args as [string | number | Date]))
  }

  static override now(): number {
    return RealDate.now() + skewMs
  }
}

/** The true time, whatever skew is installed. */
export function realNow(): number {
  return RealDate.now()
}

/** Install the skewable `Date`. Returns a restore function. */
export function installSkewableClock(): () => void {
  globalThis.Date = SkewedDate as DateConstructor
  return () => {
    globalThis.Date = RealDate
    skewMs = 0
  }
}

/** Shift this process's wall clock by `ms` (negative runs behind). */
export function setClockSkew(ms: number): void {
  skewMs = ms
}
