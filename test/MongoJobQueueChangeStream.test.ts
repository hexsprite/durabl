import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import type { Collection, Db } from 'mongodb'

import { MongoJobQueue } from '../src/backends/MongoJobQueue'
import type { JobDoc } from '../src/types'

import {
  closeMongo,
  getMongo,
  uniqueCollectionName,
} from './mongoHelper'

/** Retry sentinel inserts every 2s until the stream delivers one — a single
 * insert can race the cursor's async init(), so we keep probing. */
async function waitForStreamReady(be: MongoJobQueue): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let done = false
    const totalTimer = setTimeout(() => {
      if (!done) {
        done = true
        clearInterval(probeInterval)
        unsub?.()
        reject(new Error('stream readiness probe timed out'))
      }
    }, 15000)

    const unsub = be.onJobAvailable((type: string) => {
      if (type === '__stream_probe__' && !done) {
        done = true
        clearTimeout(totalTimer)
        clearInterval(probeInterval)
        unsub?.()
        resolve()
      }
    })

    const sendProbe = (): void => {
      if (!done) {
        be.enqueue('__stream_probe__', {}).catch(() => {
          /* noop */
        })
      }
    }
    sendProbe()
    const probeInterval = setInterval(sendProbe, 2000)
  })
}

describe('MongoJobQueue change streams', () => {
  let db: Db
  let isReplicaSet = false
  let backend: MongoJobQueue
  let collection: Collection<JobDoc>

  beforeAll(async () => {
    ;({ db, isReplicaSet } = await getMongo())
    if (!isReplicaSet) {
      // Change streams require a replica set. With MONGO_URL pointed at a
      // standalone server, the whole suite is skipped rather than failing.
      // eslint-disable-next-line no-console
      console.warn(
        'change stream suite skipped — MONGO_URL is not a replica set',
      )
    }
  })

  beforeEach(async () => {
    if (!isReplicaSet) return
    collection = db.collection<JobDoc>(uniqueCollectionName('test_jobs_cs'))
    backend = new MongoJobQueue({
      db,
      collectionName: collection.collectionName,
      useChangeStreams: true,
    })
    await backend.startup()
    await waitForStreamReady(backend)
  })

  afterEach(async () => {
    if (!isReplicaSet) return
    await backend.shutdown()
    await collection.drop().catch(() => {
      /* ignore */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  it('notifies listeners within ~2s of a new pending job', async () => {
    if (!isReplicaSet) return

    let notifiedType: string | null = null
    let unsubscribe: (() => void) | null = null
    const waitForEvent = new Promise<string>((resolve) => {
      unsubscribe = backend.onJobAvailable((type: string) => {
        if (type.startsWith('__')) return // skip probe events
        notifiedType = type
        resolve(type)
      })
    })
    expect(unsubscribe).not.toBeNull()

    const start = Date.now()
    await backend.enqueue('csTestJob', { value: 42 })
    const race = await Promise.race([
      waitForEvent,
      new Promise<'timeout'>((resolve) =>
        setTimeout(() => resolve('timeout'), 10000),
      ),
    ])

    const elapsed = Date.now() - start
    expect(race).toBe('csTestJob')
    expect(notifiedType).toBe('csTestJob')
    expect(elapsed).toBeLessThan(2000) // sub-100ms real pickup; probe ensures stream is live
  })

  it('notifies listeners when a failed job is reset to pending on retry', async () => {
    if (!isReplicaSet) return

    const jobId = await backend.enqueue('csRetryJob', { value: 1 })
    expect(jobId).not.toBeNull()
    const claimed = await backend.claimNext('csRetryJob')
    expect(claimed).not.toBeNull()

    // Subscribe AFTER initial insert so we only observe the retry event.
    const events: string[] = []
    let retryUnsub: (() => void) | null = null
    const retryEvent = new Promise<string>((resolve) => {
      retryUnsub = backend.onJobAvailable((type: string) => {
        if (type.startsWith('__')) return // skip probe events
        events.push(type)
        resolve(type)
      })
    })
    expect(retryUnsub).not.toBeNull()

    // fail() flips status back to 'pending', which the watcher matches.
    await backend.fail(claimed!.id, 'transient failure')

    const race = await Promise.race([
      retryEvent,
      new Promise<'timeout'>((resolve) =>
        setTimeout(() => resolve('timeout'), 4000),
      ),
    ])

    expect(race).toBe('csRetryJob')
    expect(events).toContain('csRetryJob')
  })

  it('unsubscribes cleanly', async () => {
    if (!isReplicaSet) return

    // Subscribe then immediately unsubscribe — a second listener confirms delivery.
    let callCount = 0
    const unsubscribe = backend.onJobAvailable(() => {
      callCount++
    })
    expect(unsubscribe).not.toBeNull()
    unsubscribe!()

    const eventDelivered = new Promise<void>((resolve) => {
      backend.onJobAvailable((type: string) => {
        if (type.startsWith('__')) return // skip probe events
        resolve()
      })
    })

    await backend.enqueue('csTestJob', {})
    await Promise.race([
      eventDelivered,
      new Promise<void>((_, reject) =>
        setTimeout(
          () => reject(new Error('change stream event never delivered')),
          10000,
        ),
      ),
    ])

    expect(callCount).toBe(0)
  })

  it('handles shutdown() during startup() without leaking the watcher', async () => {
    if (!isReplicaSet) return

    const name = uniqueCollectionName('test_jobs_cs_shutdown_race')
    const col = db.collection<JobDoc>(name)
    const be = new MongoJobQueue({
      db,
      collectionName: name,
      useChangeStreams: true,
    })
    try {
      // Race shutdown() against mid-await watcher.start().
      const startupPromise = be.startup()
      await be.shutdown()
      await startupPromise.catch(() => {
        /* expected */
      })
      expect(be.isChangeStreamsActive()).toBe(false)
    } finally {
      await col.drop().catch(() => {
        /* ignore */
      })
    }
  })

  it('flushes listeners registered concurrently with startup()', async () => {
    if (!isReplicaSet) return

    // Exercises the pending-listener buffer: mid-startup listener must be flushed.
    const name = uniqueCollectionName('test_jobs_cs_concurrent')
    const concurrentCollection = db.collection<JobDoc>(name)
    const concurrentBackend = new MongoJobQueue({
      db,
      collectionName: name,
      useChangeStreams: true,
    })

    try {
      const startupPromise = concurrentBackend.startup() // don't await yet
      // Register listener while startup() is in flight — lands in pendingListeners buffer
      // since watcher is still null (startup hasn't passed its first await).
      let notifiedType: string | null = null
      const waitForEvent = new Promise<string>((resolve) => {
        concurrentBackend.onJobAvailable((type: string) => {
          if (type.startsWith('__')) return // skip probe events
          notifiedType = type
          resolve(type)
        })
      })

      await startupPromise
      await waitForStreamReady(concurrentBackend)

      await concurrentBackend.enqueue('csConcurrentJob', {})
      const race = await Promise.race([
        waitForEvent,
        new Promise<'timeout'>((resolve) =>
          setTimeout(() => resolve('timeout'), 4000),
        ),
      ])

      expect(race).toBe('csConcurrentJob')
      expect(notifiedType).toBe('csConcurrentJob')
    } finally {
      await concurrentBackend.shutdown()
      await concurrentCollection.drop().catch(() => {
        /* ignore */
      })
    }
  })
})
