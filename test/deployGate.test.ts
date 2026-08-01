/**
 * Deploy-gate suite.
 *
 * The behaviour under test is deploy *ordering*: during a rolling deploy an
 * older machine can boot beside a newer one, and its unconditional startup
 * work (indexes, validators, migrations) would otherwise re-apply an older
 * schema over the newer one. The gate must let the newest build through and
 * turn the older one into a no-op.
 *
 * The identical-timestamp case is asserted as *both run* on purpose — the gate
 * is a skip, not a lock (see `src/deployGate.ts`). If that ever becomes
 * exclusion, this test is the one that should fail and force the decision.
 */
import { promises as fs } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { createDeployGate, runIfLatestBuild } from '../src/deployGate'
import type { DeploymentDoc } from '../src/deployGate'
import type { Logger } from '../src/logger'

import { closeMongo, getMongo, uniqueCollectionName } from './mongoHelper'

const OLD = new Date('2026-01-01T00:00:00.000Z')
const NEW = new Date('2026-02-01T00:00:00.000Z')

interface CapturingLogger extends Logger {
  warnings: unknown[]
}

/** Silent logger that keeps the warn payloads so skips can be asserted. */
function capturingLogger(warnings: unknown[] = []): CapturingLogger {
  return {
    debug: () => {},
    info: () => {},
    warn: (objOrMsg: unknown) => {
      warnings.push(objOrMsg)
    },
    error: () => {},
    child: () => capturingLogger(warnings),
    warnings,
  }
}

describe('deploy gate', () => {
  let db: Db
  let collectionName: string
  let collection: Collection<DeploymentDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    collectionName = uniqueCollectionName('deployments')
    collection = db.collection<DeploymentDoc>(collectionName)
  })

  afterEach(async () => {
    await collection.drop().catch(() => {
      /* collection may never have been created */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  const gateFor = (buildTimestamp: Date, revision?: string) =>
    createDeployGate({
      db,
      collectionName,
      buildTimestamp,
      revision,
      logger: capturingLogger(),
    })

  it('runs hooks and records the build on a first-ever boot', async () => {
    const gate = gateFor(OLD, 'sha-old')
    let ran = 0
    gate(async () => {
      ran += 1
    })

    const result = await gate.run()

    expect(result).toMatchObject({
      ran: true,
      previousTimestamp: null,
      hooksRun: 1,
    })
    expect(ran).toBe(1)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.buildTimestamp).toEqual(OLD)
    expect(row?.revision).toBe('sha-old')
  })

  it('runs hooks and advances the record when the build is newer', async () => {
    await gateFor(OLD, 'sha-old').run()

    const gate = gateFor(NEW, 'sha-new')
    let ran = 0
    gate(async () => {
      ran += 1
    })
    const result = await gate.run()

    expect(result.ran).toBe(true)
    expect(result.previousTimestamp).toEqual(OLD)
    expect(ran).toBe(1)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.buildTimestamp).toEqual(NEW)
    expect(row?.revision).toBe('sha-new')
  })

  it('skips hooks and leaves the record alone when the build is older', async () => {
    await gateFor(NEW, 'sha-new').run()

    const logger = capturingLogger()
    const gate = createDeployGate({
      db,
      collectionName,
      buildTimestamp: OLD,
      revision: 'sha-old',
      logger,
    })
    let ran = 0
    gate(async () => {
      ran += 1
    })
    const result = await gate.run()

    expect(result).toMatchObject({ ran: false, hooksRun: 0 })
    expect(result.previousTimestamp).toEqual(NEW)
    expect(ran).toBe(0)
    // The newer machine's record survives the older machine's boot.
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.buildTimestamp).toEqual(NEW)
    expect(row?.revision).toBe('sha-new')
    expect(logger.warnings).toHaveLength(1)
    expect(logger.warnings[0]).toMatchObject({
      buildTimestamp: OLD,
      previousTimestamp: NEW,
      previousRevision: 'sha-new',
    })
  })

  it('lets both processes run when the build timestamps are identical', async () => {
    // Documented gap: skip, not lock. Hooks must be idempotent.
    let ran = 0
    for (const _ of [1, 2]) {
      const gate = gateFor(OLD)
      gate(async () => {
        ran += 1
      })
      const result = await gate.run()
      expect(result.ran).toBe(true)
    }

    expect(ran).toBe(2)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.buildTimestamp).toEqual(OLD)
  })

  it('records the newest build when two boots race the compare-and-swap', async () => {
    const older = gateFor(OLD, 'sha-old')
    const newer = gateFor(NEW, 'sha-new')
    let ran = 0
    const hook = async () => {
      ran += 1
    }
    older(hook)
    newer(hook)

    // Neither sees the other's row, so both run; the record must still end up
    // on the newer build regardless of which write lands last.
    const [a, b] = await Promise.all([older.run(), newer.run()])

    expect(a.ran).toBe(true)
    expect(b.ran).toBe(true)
    expect(ran).toBe(2)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.buildTimestamp).toEqual(NEW)
    expect(row?.revision).toBe('sha-new')
    expect(await collection.countDocuments()).toBe(1)
  })

  it('never moves the record backward when the older boot writes last', async () => {
    const older = gateFor(OLD, 'sha-old')
    const newer = gateFor(NEW, 'sha-new')

    // The older process reads before the newer one exists, but is still inside
    // its hooks when the newer one records — so its CAS runs afterwards.
    older(async () => {
      await newer.run()
    })
    await older.run()

    const row = await collection.findOne({ _id: 'default' })
    expect(row?.buildTimestamp).toEqual(NEW)
    expect(row?.revision).toBe('sha-new')
  })

  it('runs hooks in registration order and deduplicates a repeated hook', async () => {
    const order: string[] = []
    const first = async () => {
      order.push('first')
    }
    const gate = gateFor(OLD)
    gate(first)
    gate(async () => {
      order.push('second')
    })
    gate(first) // same reference — registered once

    const result = await gate.run()

    expect(order).toEqual(['first', 'second'])
    expect(result.hooksRun).toBe(2)
  })

  it('propagates a failing hook and does not claim the deploy', async () => {
    const gate = gateFor(NEW)
    gate(async () => {
      throw new Error('migration blew up')
    })

    await expect(gate.run()).rejects.toThrow('migration blew up')
    expect(await collection.findOne({ _id: 'default' })).toBeNull()
  })

  it('falls back to the entrypoint mtime for the build version', async () => {
    const entrypoint = join(tmpdir(), `durabl-entrypoint-${Date.now()}.js`)
    await fs.writeFile(entrypoint, '// build artifact\n')
    await fs.utimes(entrypoint, NEW, NEW)

    try {
      const gate = createDeployGate({
        db,
        collectionName,
        entrypoint,
        logger: capturingLogger(),
      })
      const result = await gate.run()

      expect(result.buildTimestamp).toEqual(NEW)
      const row = await collection.findOne({ _id: 'default' })
      expect(row?.buildTimestamp).toEqual(NEW)
    } finally {
      await fs.rm(entrypoint, { force: true })
    }
  })

  it('keeps separate deploymentIds independent', async () => {
    await createDeployGate({
      db,
      collectionName,
      deploymentId: 'web',
      buildTimestamp: NEW,
      logger: capturingLogger(),
    }).run()

    const workers = createDeployGate({
      db,
      collectionName,
      deploymentId: 'workers',
      buildTimestamp: OLD,
      logger: capturingLogger(),
    })
    let ran = 0
    workers(async () => {
      ran += 1
    })

    // A newer *web* deploy must not gate the *workers* deploy.
    expect((await workers.run()).ran).toBe(true)
    expect(ran).toBe(1)
    expect(await collection.countDocuments()).toBe(2)
  })

  it('runs a single hook through the runIfLatestBuild convenience', async () => {
    let ran = 0
    const first = await runIfLatestBuild(
      { db, collectionName, buildTimestamp: NEW, logger: capturingLogger() },
      async () => {
        ran += 1
      },
    )
    const second = await runIfLatestBuild(
      { db, collectionName, buildTimestamp: OLD, logger: capturingLogger() },
      async () => {
        ran += 1
      },
    )

    expect(first.ran).toBe(true)
    expect(second.ran).toBe(false)
    expect(ran).toBe(1)
  })
})
