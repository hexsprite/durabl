/**
 * Version gate — run startup work only when this process carries the newest
 * version.
 *
 * A rolling deploy (Fly, k8s, anything that overlaps machines) can run two
 * processes at different versions at the same time. Startup work that runs
 * unconditionally on every boot — index creation, `collMod` validators,
 * migrations — is therefore re-applied by whichever process happens to boot
 * last, and an OLDER process coming up beside a newer one silently reverts
 * the newer schema. Rolling deploys are the motivating case, but the same
 * shape shows up anywhere several processes race to apply startup work and
 * only the one carrying the newest version should win.
 *
 * The gate records the newest version any process has observed in a
 * singleton Mongo document and skips the hooks when the booting process is
 * behind it.
 *
 * ```typescript
 * const runIfNewest = createVersionGate({
 *   db,
 *   version: await buildTimestampFromFile(),
 * })
 *
 * runIfNewest(async () => { await reconcileIndexes() })
 * runIfNewest(async () => { await runMigrations() })
 *
 * await runIfNewest.run() // once, at the end of startup
 * ```
 *
 * ## Skip, not lock — a deliberate gap
 *
 * Two processes carrying the *identical* version both run the hooks; nothing
 * serialises them. That is by design. The problem being solved is ordering
 * (an old version must not undo a newer one's work), not exclusion, and
 * registered hooks are required to be idempotent regardless — they run on
 * every boot of the newest version anyway. durabl has lease primitives that
 * could add exclusion, but a lock on the startup path is a new way to hang a
 * boot, so it is not taken by default.
 *
 * ## Choosing a version
 *
 * `version` is required and must be an ordered value: a number or a `Date`.
 * {@link buildTimestampFromFile} gives you the mtime of the server bundle as
 * epoch milliseconds — cheap, monotonic per deploy, and needs no build-time
 * codegen — but any monotonically increasing value works: a CI run number, an
 * incrementing build number, a timestamp stamped at build time. Pass
 * `revision` to record a git SHA or image digest alongside it for debugging;
 * `revision` is metadata only and is never compared, because it is not
 * ordered.
 *
 * Prior art: `deathandmayhem/jolly-roger`, `imports/server/runIfLatestBuild.ts`.
 */
import { promises as fs } from 'node:fs'

import type { Collection, Db } from 'mongodb'

import { defaultLogger, type Logger } from './logger'

/** Singleton document recording the newest version observed so far. */
export interface VersionGateDoc {
  _id: string
  /** Version of the newest process that has run its startup hooks, as epoch ms. */
  version: number
  /** Caller-supplied build identity (git SHA, image digest), if any. */
  revision?: string
  /** When the row was last moved forward. Diagnostics only. */
  updatedAt: Date
}

export interface VersionGateOptions {
  /** Database handle from a connected `MongoClient`. */
  db: Db
  /**
   * This process's version. A number is epoch milliseconds; a `Date` is
   * converted to epoch milliseconds on entry — see the module doc comment
   * for why only one numeric form is ever stored or compared.
   */
  version: number | Date
  /** Collection holding the singleton row. Default: `'versionGates'`. */
  collectionName?: string
  /** `_id` of the singleton row. Default: `'default'`. */
  gateId?: string
  /** Build identity recorded alongside the version (git SHA, image digest). */
  revision?: string
  /** Injectable logger. Default: console. */
  logger?: Logger
}

export interface VersionGateResult {
  /** Did the hooks run? `false` means a newer version got here first. */
  ran: boolean
  /** The version this process resolved for itself, as epoch ms. */
  version: number
  /** The version already on record, or `null` when this is the first boot. */
  previousVersion: number | null
  /** How many registered hooks were executed. */
  hooksRun: number
}

export interface VersionGate {
  /**
   * Register a hook to run at {@link VersionGate.run}, but only if this
   * process carries the newest version. Hooks run in registration order;
   * registering the same function reference twice registers it once.
   */
  (hook: () => Promise<void>): void
  /**
   * Run the registered hooks if this process's version is the newest seen,
   * then move the recorded version forward.
   *
   * A throwing hook propagates and the recorded version is left alone, so the
   * boot fails loudly rather than claiming a version that did not finish.
   */
  run(): Promise<VersionGateResult>
}

/** Mongo's unique-index violation, across driver/server error shapes. */
function isDuplicateKeyError(err: unknown): boolean {
  if (!(err instanceof Error)) return false
  const { code } = err as { code?: unknown }
  return code === 11000 || err.message.includes('E11000')
}

/**
 * Normalize `version` to epoch milliseconds immediately, before any
 * comparison or write.
 *
 * MongoDB compares across BSON types by type bracketing, not by value: a
 * `$lt` guard mixing a stored `Date` and a supplied `number` (or vice versa)
 * would not compare the way a caller expects, because every value of one
 * BSON type sorts against every value of the other by type, not by the
 * instant each one names. Storing and comparing a single numeric type keeps
 * the CAS guard `{ _id: gateId, version: { $lt: ours } }` correct regardless
 * of which form — `number` or `Date` — the caller passed in.
 */
function normalizeVersion(name: string, version: number | Date): number {
  const ms = typeof version === 'number' ? version : version.getTime()
  if (!Number.isFinite(ms)) {
    throw new Error(
      `VersionGateOptions.${name} must be a finite number (epoch milliseconds) or a valid Date, got ${ms}`,
    )
  }
  return ms
}

/**
 * Stat `path` and return its mtime as epoch milliseconds. Convenience for
 * driving {@link VersionGateOptions.version} from a build artifact's
 * filesystem timestamp instead of a CI-supplied identity.
 *
 * Defaults to `process.argv[1]`, the running entrypoint.
 */
export async function buildTimestampFromFile(path?: string): Promise<number> {
  const entrypoint = path ?? process.argv[1]
  if (!entrypoint) {
    throw new Error(
      'durabl version gate: no path given and process.argv[1] is empty — ' +
        'pass a path explicitly.',
    )
  }
  const stat = await fs.stat(entrypoint)
  return stat.mtime.getTime()
}

/**
 * Move the singleton row forward to `version`, never backward.
 *
 * The guarded update is the compare-and-swap. It matches nothing when the row
 * is absent *or* already at/ahead of us, so an absent row is handled by an
 * insert — and a losing insert re-runs the guarded update, because the process
 * that beat us to creation may have written an older version than ours.
 */
async function recordLatestVersion(
  collection: Collection<VersionGateDoc>,
  gateId: string,
  version: number,
  revision: string | undefined,
): Promise<void> {
  const fields = {
    version,
    updatedAt: new Date(),
    ...(revision === undefined ? {} : { revision }),
  }
  const advance = (): Promise<{ matchedCount: number }> =>
    collection.updateOne(
      { _id: gateId, version: { $lt: version } },
      { $set: fields },
    )

  const advanced = await advance()
  if (advanced.matchedCount > 0) return

  try {
    await collection.insertOne({ _id: gateId, ...fields })
  } catch (err) {
    if (!isDuplicateKeyError(err)) throw err
    await advance()
  }
}

/**
 * Build a {@link VersionGate} over `options.db`.
 *
 * The returned value is callable (register a hook) and carries `run()`.
 */
export function createVersionGate(options: VersionGateOptions): VersionGate {
  const version = normalizeVersion('version', options.version)
  const collection = options.db.collection<VersionGateDoc>(
    options.collectionName ?? 'versionGates',
  )
  const gateId = options.gateId ?? 'default'
  const logger = (options.logger ?? defaultLogger).child({
    category: 'VersionGate',
  })
  // A Set, so a module registering the same hook reference twice runs it once.
  const hooks = new Set<() => Promise<void>>()

  const gate = (hook: () => Promise<void>): void => {
    hooks.add(hook)
  }

  gate.run = async (): Promise<VersionGateResult> => {
    const previous = await collection.findOne({ _id: gateId })
    const previousVersion = previous?.version ?? null

    if (previousVersion !== null && previousVersion > version) {
      logger.warn(
        {
          version,
          previousVersion,
          previousRevision: previous?.revision,
        },
        'Skipping startup hooks: a newer version has already run',
      )
      return { ran: false, version, previousVersion, hooksRun: 0 }
    }

    let hooksRun = 0
    for (const hook of hooks) {
      await hook()
      hooksRun += 1
    }

    await recordLatestVersion(collection, gateId, version, options.revision)
    logger.info(
      { version, previousVersion, hooksRun },
      'Ran startup hooks as the newest version',
    )
    return { ran: true, version, previousVersion, hooksRun }
  }

  return gate
}

/**
 * One-shot convenience: run `hook` only if this process carries the newest
 * version.
 *
 * Equivalent to creating a gate, registering one hook and running it. Use
 * {@link createVersionGate} when several modules need to register.
 */
export async function runIfNewestVersion(
  options: VersionGateOptions,
  hook: () => Promise<void>,
): Promise<VersionGateResult> {
  const gate = createVersionGate(options)
  gate(hook)
  return gate.run()
}
