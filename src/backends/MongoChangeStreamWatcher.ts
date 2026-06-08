/**
 * MongoChangeStreamWatcher — watches a Mongo collection for
 * insert/update/replace events where `fullDocument.status === 'pending'`
 * and notifies listeners with the job type. Used by `MongoJobQueue`.
 *
 * On stream error/unexpected close, reconnects with exponential backoff
 * (1s → 30s). After reconnect, dispatches a catch-up sentinel (`''`) so
 * processors drain jobs that landed during the gap. Backoff resets only
 * after a real event, so open-then-fail loops still back off.
 *
 * Requires a Mongo replica set. `start()` probes via `hello` and throws
 * on non-replica-set servers so callers can degrade to poll-only.
 */
import type {
  ChangeStream,
  ChangeStreamDocument,
  Collection,
  Db,
  ResumeToken,
  Timestamp,
} from 'mongodb'

import type { Logger } from '../logger'
import type { JobDoc } from '../types'

type Listener = (type: string) => void

const INITIAL_RECONNECT_DELAY_MS = 1000
const MAX_RECONNECT_DELAY_MS = 30000

export class MongoChangeStreamWatcher {
  private collection: Collection<JobDoc>
  private db: Db
  private log: Logger
  private listeners: Set<Listener> = new Set()
  private stream: ChangeStream | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectDelayMs = INITIAL_RECONNECT_DELAY_MS
  private resumeToken?: ResumeToken
  /** Cluster time pinned as `startAtOperationTime` — fixes cursor start
   * before any future insert so events can't race materialization. */
  private startAtOperationTime?: Timestamp
  private stopped = false
  /** True once `open()` has succeeded at least once. Lets `open()`
   * distinguish initial open (no catch-up needed, startup poll covers it)
   * from reconnect (dispatch catch-up to close the pickup gap). */
  private hasOpenedOnce = false

  constructor(collection: Collection<JobDoc>, db: Db, log: Logger) {
    this.collection = collection
    this.db = db
    this.log = log.child({ category: 'MongoChangeStreamWatcher' })
  }

  addListener(listener: Listener): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  /**
   * Remove a listener without needing the closure returned by `addListener`.
   * Used by `MongoJobQueue` so that an unsubscribe closure captured before
   * `startup()` can still reach into the live watcher after the pending
   * buffer has been flushed.
   */
  removeListener(listener: Listener): void {
    this.listeners.delete(listener)
  }

  /** Probe for replica-set support, then open the change stream. Throws
   * on non-rs so callers can degrade to poll-only. */
  async start(): Promise<void> {
    if (this.stopped) return
    await this.probeReplicaSet()
    await this.open()
  }

  async stop(): Promise<void> {
    this.stopped = true
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    const stream = this.stream
    this.stream = null
    if (stream) {
      try {
        await stream.close()
      } catch (err) {
        this.log.warn({ err }, 'error closing change stream on shutdown')
      }
    }
    this.reconnectDelayMs = INITIAL_RECONNECT_DELAY_MS
    this.listeners.clear()
  }

  /** Probe the injected db for replica-set support and pin a cluster time. */
  private async probeReplicaSet(): Promise<void> {
    const info = (await this.db.command({ hello: 1 })) as {
      setName?: string
      operationTime?: Timestamp
    }
    if (!info.setName) {
      throw new Error(
        'MongoChangeStreamWatcher: not a replica set (rs required for change streams)',
      )
    }
    // Pin cursor start strictly before any future insert — otherwise the
    // real start is whenever the server processes the aggregate, which on
    // loaded CI can race past a just-landed oplog entry.
    this.startAtOperationTime = info.operationTime
  }

  private async open(): Promise<void> {
    if (this.stopped) return

    // Re-probe for a fresh cluster time when both start hints are gone
    // (reconnect-after-error). Otherwise we'd race cursor materialization.
    if (
      this.resumeToken === undefined &&
      this.startAtOperationTime === undefined
    ) {
      try {
        await this.probeReplicaSet()
      } catch (err) {
        this.log.warn({ err }, 'failed to refresh operation time; will retry')
        this.scheduleReconnect()
        return
      }
      // stop() may have run during the probe await. Bail before opening a
      // stream that nobody will ever close.
      if (this.stopped) return
    }

    // `fullDocument: 'updateLookup'` below attaches the post-image on
    // update/replace; for inserts the post-image is the inserted doc.
    // Server applies the lookup before $match, so the status filter is
    // valid across all three op types.
    const pipeline = [
      {
        $match: {
          operationType: { $in: ['insert', 'update', 'replace'] },
          'fullDocument.status': 'pending',
        },
      },
    ]

    // `resumeAfter` (exact event continuity) and `startAtOperationTime`
    // (race-proof start pin) are mutually exclusive. Prefer the token on
    // reconnect; fall back to cluster time on initial/fresh open.
    const watchOpts: {
      fullDocument: 'updateLookup'
      resumeAfter?: ResumeToken
      startAtOperationTime?: Timestamp
    } = {
      fullDocument: 'updateLookup',
    }
    if (this.resumeToken !== undefined) {
      watchOpts.resumeAfter = this.resumeToken
    } else if (this.startAtOperationTime !== undefined) {
      watchOpts.startAtOperationTime = this.startAtOperationTime
    }

    let stream: ChangeStream
    try {
      stream = this.collection.watch(pipeline, watchOpts)
    } catch (err) {
      this.log.warn({ err }, 'failed to open change stream; will retry')
      // Clear both start hints so the reconnect re-probes for a fresh
      // cluster time. Otherwise a stale/expired token or operation time
      // (e.g. oplog rollover) would fail the retry for the same reason
      // forever, permanently killing the stream.
      this.resumeToken = undefined
      this.startAtOperationTime = undefined
      this.scheduleReconnect()
      return
    }

    // MongoDB 6.x locks out EventEmitter mode once tryNext() is called, so
    // go straight to EventEmitter. startAtOperationTime guarantees no
    // events are missed between watch() and cursor materialization.
    stream.on('change', (event: ChangeStreamDocument) =>
      this.handleChange(event),
    )
    // On error: drop hints and reconnect. Guarantees liveness against
    // InvalidResumeToken / ChangeStreamHistoryLost. Poll loop backstops gap.
    stream.on('error', (err) => {
      const code = (err as { code?: number })?.code
      this.log.warn(
        { err, code },
        'change stream error; dropping token, reconnecting',
      )
      this.resumeToken = undefined
      this.startAtOperationTime = undefined
      this.scheduleReconnect()
    })
    // Non-error close (`invalidate`, cursor expiry) would otherwise kill
    // the watcher silently. Guard: skip if scheduleReconnect() already ran
    // for this stream (error handler nulled this.stream) to avoid spurious
    // "closed unexpectedly" warnings on every error event.
    stream.on('close', () => {
      if (this.stopped) return
      if (this.stream !== stream) return
      this.log.warn('change stream closed unexpectedly; reconnecting')
      this.resumeToken = undefined
      this.startAtOperationTime = undefined
      this.scheduleReconnect()
    })

    this.stream = stream
    this.log.info('change stream watcher started')
    // Backoff resets in handleChange(), not here — prevents open-then-fail
    // loops at the 1s floor. Trade-off: on quiet queues backoff ratchets
    // toward 30s (no events to reset it); the poll loop backstops pickup so
    // only reconnect latency suffers. Time-based reset if this becomes an issue.

    // After a reconnect (not initial open), fan out a catch-up sentinel so
    // processors re-poll for jobs that landed during the gap. Initial open
    // is covered by the startup poll; reconnects otherwise rely on the
    // safety-net poll. JobQueue treats an empty type as "try all processors".
    if (this.hasOpenedOnce) this.dispatchCatchUp()
    else this.hasOpenedOnce = true
  }

  private dispatchCatchUp(): void {
    for (const listener of this.listeners) {
      try {
        listener('')
      } catch (err) {
        this.log.warn({ err }, 'catch-up listener threw')
      }
    }
  }

  private handleChange(event: ChangeStreamDocument): void {
    // Bail if stop() ran during the open() → handleChange(primed) window.
    if (this.stopped) return
    try {
      this.resumeToken = event._id
      // Stream has proved useful — delivered a real event. Reset the
      // reconnect backoff so the next transient blip bounces back quickly.
      // See `open()` for why this isn't done on successful open.
      this.reconnectDelayMs = INITIAL_RECONNECT_DELAY_MS
      // Pipeline only passes insert/update/replace; all carry fullDocument.
      const doc = (event as { fullDocument?: JobDoc }).fullDocument
      // Guard: doc may be null (deleted between oplog event and lookup) or
      // no longer pending (claimed between event and post-image lookup).
      if (!doc || doc.status !== 'pending') return
      for (const listener of this.listeners) {
        try {
          listener(doc.type)
        } catch (err) {
          this.log.warn({ err }, 'change stream listener threw')
        }
      }
    } catch (err) {
      this.log.warn({ err }, 'error handling change stream event')
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer) return

    const existing = this.stream
    this.stream = null
    if (existing) {
      existing.close().catch((err) => {
        this.log.warn({ err }, 'error closing failed change stream')
      })
    }

    const delay = this.reconnectDelayMs
    // Pre-compute the next backoff now. Safe because watcher instances are
    // not reused after stop() — a fresh instance starts at INITIAL_RECONNECT.
    this.reconnectDelayMs = Math.min(
      this.reconnectDelayMs * 2,
      MAX_RECONNECT_DELAY_MS,
    )
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.open().catch((err) => {
        this.log.warn({ err }, 'error reopening change stream')
      })
    }, delay)
  }
}
