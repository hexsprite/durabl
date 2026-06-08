/**
 * Mongo test harness.
 *
 * Change streams require a replica set, so the default path spins up an
 * in-memory single-node replica set via `mongodb-memory-server`. Point
 * `MONGO_URL` at a real replica-set cluster to test against that instead
 * (the URL must already be a replica set for the change-stream suite).
 */
import { MongoClient, type Db } from 'mongodb'
import { MongoMemoryReplSet } from 'mongodb-memory-server'

let replset: MongoMemoryReplSet | null = null
let client: MongoClient | null = null

export interface MongoTestHandle {
  db: Db
  /** True when the connected server is a replica set (change streams work). */
  isReplicaSet: boolean
}

/** Connect once per process. Reused across suites. */
export async function getMongo(): Promise<MongoTestHandle> {
  if (client) {
    return { db: client.db('durabl_test'), isReplicaSet: await isReplSet(client) }
  }

  const url = process.env.MONGO_URL
  if (url) {
    client = await MongoClient.connect(url)
  } else {
    replset = await MongoMemoryReplSet.create({ replSet: { count: 1 } })
    client = await MongoClient.connect(replset.getUri())
  }

  return {
    db: client.db('durabl_test'),
    isReplicaSet: await isReplSet(client),
  }
}

async function isReplSet(c: MongoClient): Promise<boolean> {
  try {
    const info = (await c.db().admin().command({ hello: 1 })) as {
      setName?: string
    }
    return Boolean(info.setName)
  } catch {
    return false
  }
}

/** Tear down the shared connection + in-memory server. */
export async function closeMongo(): Promise<void> {
  await client?.close()
  client = null
  if (replset) {
    await replset.stop()
    replset = null
  }
}

/** Unique collection name so parallel suites never collide. */
let counter = 0
export function uniqueCollectionName(prefix = 'jobs'): string {
  counter += 1
  return `${prefix}_${Date.now()}_${counter}`
}
