/**
 * The Mongo server's clock, read locally.
 *
 * Leases and schedules compare times written by different hosts. If each host
 * used its own clock, a worker running behind the reaper would look stale
 * while alive, and its job would run twice. Every host instead tracks the
 * offset between its clock and the server's, and stamps `local + offset`.
 *
 * The offset is measured, not read per call, so claim filters keep plain
 * `Date` values and their index plans (no `$$NOW` in `$expr`).
 */
import type { Logger } from '../logger'

/** How often the offset is measured again. Clocks drift; NTP steps them. */
export const SERVER_CLOCK_REFRESH_MS = 5 * 60_000

/** The part of a `hello` reply the clock reads. */
export interface HelloReply {
  localTime?: unknown
}

export interface ServerClockOptions {
  /** Runs `{ hello: 1 }` against the server. */
  hello: () => Promise<HelloReply>
  logger: Logger
  /** Local clock in epoch ms. Default: `Date.now`. Injected by tests. */
  localNow?: () => number
  /** Re-measure interval. Default: {@link SERVER_CLOCK_REFRESH_MS}. */
  refreshIntervalMs?: number
  /**
   * Measure the server offset at all. Default: `true`. When `false` the clock
   * is the local clock: offset fixed at zero, no `hello`, no timer.
   */
  measure?: boolean
}

export class ServerClock {
  private offset = 0
  private timer: ReturnType<typeof setInterval> | null = null
  private readonly hello: () => Promise<HelloReply>
  private readonly logger: Logger
  private readonly localNow: () => number
  private readonly refreshIntervalMs: number
  private readonly measure: boolean

  constructor(options: ServerClockOptions) {
    this.hello = options.hello
    this.logger = options.logger
    this.localNow = options.localNow ?? (() => Date.now())
    this.refreshIntervalMs = options.refreshIntervalMs ?? SERVER_CLOCK_REFRESH_MS
    this.measure = options.measure ?? true
  }

  /** Server time minus local time, in ms. Zero until the first measurement. */
  get offsetMs(): number {
    return this.offset
  }

  /** Local clock reading in epoch ms (the clock the offset is relative to). */
  localMs(): number {
    return this.localNow()
  }

  /** Current server time in epoch ms. */
  nowMs(): number {
    return this.localNow() + this.offset
  }

  /** Current server time. */
  now(): Date {
    return this.fromLocal(this.localNow())
  }

  /** Server time that corresponds to a local clock reading. */
  fromLocal(localMs: number): Date {
    return new Date(localMs + this.offset)
  }

  /**
   * Measure the offset once. The server read `localTime` somewhere inside the
   * round trip, so compare it with the round trip's midpoint: the error is at
   * most half the round trip. On failure the last offset stays in force.
   */
  async refresh(): Promise<void> {
    if (!this.measure) return
    const sentAt = this.localNow()
    let reply: HelloReply
    try {
      reply = await this.hello()
    } catch (err) {
      this.logger.warn(
        { err, offsetMs: this.offset },
        'server clock measurement failed; keeping the last offset',
      )
      return
    }
    const receivedAt = this.localNow()
    if (!(reply.localTime instanceof Date)) {
      this.logger.warn(
        { offsetMs: this.offset },
        'hello reply has no localTime; keeping the last server clock offset',
      )
      return
    }
    this.offset = reply.localTime.getTime() - (sentAt + receivedAt) / 2
  }

  /** Measure now, then again on a timer that does not hold the process open. */
  async start(): Promise<void> {
    if (!this.measure) return
    await this.refresh()
    if (this.timer) return
    this.timer = setInterval(() => void this.refresh(), this.refreshIntervalMs)
    this.timer.unref?.()
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
  }
}
