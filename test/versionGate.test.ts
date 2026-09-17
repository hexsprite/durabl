/**
 * Version-gate suite.
 *
 * The behaviour under test is ordering: during a rolling deploy an older
 * process can boot beside a newer one, and its unconditional startup work
 * (indexes, validators, migrations) would otherwise re-apply an older schema
 * over the newer one. The gate must let the newest version through and turn
 * the older one into a no-op.
 *
 * The identical-version case is asserted as *both run* on purpose — the gate
 * is a skip, not a lock (see `src/versionGate.ts`). If that ever becomes
 * exclusion, this test is the one that should fail and force the decision.
 */
import { promises as fs } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { Collection, Db } from 'mongodb'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import {
  buildTimestampFromFile,
  createVersionGate,
  runIfNewestVersion,
} from '../src/versionGate'
import type { VersionGateDoc } from '../src/versionGate'
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

describe('version gate', () => {
  let db: Db
  let collectionName: string
  let collection: Collection<VersionGateDoc>

  beforeEach(async () => {
    ;({ db } = await getMongo())
    collectionName = uniqueCollectionName('versionGates')
    collection = db.collection<VersionGateDoc>(collectionName)
  })

  afterEach(async () => {
    await collection.drop().catch(() => {
      /* collection may never have been created */
    })
  })

  afterAll(async () => {
    await closeMongo()
  })

  const gateFor = (version: number | Date, revision?: string) =>
    createVersionGate({
      db,
      collectionName,
      version,
      revision,
      logger: capturingLogger(),
    })

  it('runs hooks and records the version on a first-ever boot', async () => {
    const gate = gateFor(OLD, 'sha-old')
    let ran = 0
    gate(async () => {
      ran += 1
    })

    const result = await gate.run()

    expect(result).toMatchObject({
      ran: true,
      previousVersion: null,
      hooksRun: 1,
    })
    expect(ran).toBe(1)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.version).toBe(OLD.getTime())
    expect(row?.revision).toBe('sha-old')
  })

  it('runs hooks and advances the record when the version is newer', async () => {
    await gateFor(OLD, 'sha-old').run()

    const gate = gateFor(NEW, 'sha-new')
    let ran = 0
    gate(async () => {
      ran += 1
    })
    const result = await gate.run()

    expect(result.ran).toBe(true)
    expect(result.previousVersion).toBe(OLD.getTime())
    expect(ran).toBe(1)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.version).toBe(NEW.getTime())
    expect(row?.revision).toBe('sha-new')
  })

  it('skips hooks and leaves the record alone when the version is older', async () => {
    await gateFor(NEW, 'sha-new').run()

    const logger = capturingLogger()
    const gate = createVersionGate({
      db,
      collectionName,
      version: OLD,
      revision: 'sha-old',
      logger,
    })
    let ran = 0
    gate(async () => {
      ran += 1
    })
    const result = await gate.run()

    expect(result).toMatchObject({ ran: false, hooksRun: 0 })
    expect(result.previousVersion).toBe(NEW.getTime())
    expect(ran).toBe(0)
    // The newer process's record survives the older process's boot.
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.version).toBe(NEW.getTime())
    expect(row?.revision).toBe('sha-new')
    expect(logger.warnings).toHaveLength(1)
    expect(logger.warnings[0]).toMatchObject({
      version: OLD.getTime(),
      previousVersion: NEW.getTime(),
      previousRevision: 'sha-new',
    })
  })

  it('lets both processes run when the versions are identical', async () => {
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
    expect(row?.version).toBe(OLD.getTime())
  })

  it('records the newest version when two boots race the compare-and-swap', async () => {
    const older = gateFor(OLD, 'sha-old')
    const newer = gateFor(NEW, 'sha-new')
    let ran = 0
    const hook = async () => {
      ran += 1
    }
    older(hook)
    newer(hook)

    // Neither sees the other's row, so both run; the record must still end up
    // on the newer version regardless of which write lands last.
    const [a, b] = await Promise.all([older.run(), newer.run()])

    expect(a.ran).toBe(true)
    expect(b.ran).toBe(true)
    expect(ran).toBe(2)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.version).toBe(NEW.getTime())
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
    expect(row?.version).toBe(NEW.getTime())
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

  it('propagates a failing hook and does not claim the version', async () => {
    const gate = gateFor(NEW)
    gate(async () => {
      throw new Error('migration blew up')
    })

    await expect(gate.run()).rejects.toThrow('migration blew up')
    expect(await collection.findOne({ _id: 'default' })).toBeNull()
  })

  it('accepts version as a plain epoch-ms number', async () => {
    const gate = createVersionGate({
      db,
      collectionName,
      version: NEW.getTime(),
      logger: capturingLogger(),
    })
    let ran = 0
    gate(async () => {
      ran += 1
    })

    const result = await gate.run()

    expect(result).toMatchObject({ ran: true, version: NEW.getTime() })
    expect(ran).toBe(1)
    const row = await collection.findOne({ _id: 'default' })
    expect(row?.version).toBe(NEW.getTime())
  })

  it('treats a Date and its equivalent epoch-ms number as the same version', async () => {
    // A Date-supplied process must not let a number-supplied process at the
    // identical instant look "newer" (or vice versa) — both forms normalize
    // to the same stored number, so a re-run at the same instant is the
    // documented skip-not-lock case (both run), not a spurious skip or a
    // spurious re-run.
    await gateFor(NEW).run() // Date

    const gate = createVersionGate({
      db,
      collectionName,
      version: NEW.getTime(), // equivalent epoch-ms number
      logger: capturingLogger(),
    })
    let ran = 0
    gate(async () => {
      ran += 1
    })
    const result = await gate.run()

    expect(result.ran).toBe(true)
    expect(result.previousVersion).toBe(NEW.getTime())
    expect(ran).toBe(1)
  })

  it('derives a build version from a file mtime via buildTimestampFromFile', async () => {
    const entrypoint = join(tmpdir(), `durabl-entrypoint-${Date.now()}.js`)
    await fs.writeFile(entrypoint, '// build artifact\n')
    await fs.utimes(entrypoint, NEW, NEW)

    try {
      const version = await buildTimestampFromFile(entrypoint)
      expect(version).toBe(NEW.getTime())

      const gate = createVersionGate({
        db,
        collectionName,
        version,
        logger: capturingLogger(),
      })
      const result = await gate.run()

      expect(result.version).toBe(NEW.getTime())
      const row = await collection.findOne({ _id: 'default' })
      expect(row?.version).toBe(NEW.getTime())
    } finally {
      await fs.rm(entrypoint, { force: true })
    }
  })

  it('keeps separate gateIds independent', async () => {
    await createVersionGate({
      db,
      collectionName,
      gateId: 'web',
      version: NEW,
      logger: capturingLogger(),
    }).run()

    const workers = createVersionGate({
      db,
      collectionName,
      gateId: 'workers',
      version: OLD,
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

  it('runs a single hook through the runIfNewestVersion convenience', async () => {
    let ran = 0
    const first = await runIfNewestVersion(
      { db, collectionName, version: NEW, logger: capturingLogger() },
      async () => {
        ran += 1
      },
    )
    const second = await runIfNewestVersion(
      { db, collectionName, version: OLD, logger: capturingLogger() },
      async () => {
        ran += 1
      },
    )

    expect(first.ran).toBe(true)
    expect(second.ran).toBe(false)
    expect(ran).toBe(1)
  })
})
