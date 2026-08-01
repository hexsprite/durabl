/**
 * Deploy gate — run startup work only when this process is the newest build.
 *
 * A rolling deploy (Fly, k8s, anything that overlaps machines) can run two
 * processes from different images at the same time. Startup work that runs
 * unconditionally on every boot — index creation, `collMod` validators,
 * migrations — is therefore re-applied by whichever process happens to boot
 * last, and an OLDER machine coming up beside a newer one silently reverts the
 * newer schema.
 *
 * The gate records the newest build timestamp any process has observed in a
 * singleton Mongo document and skips the hooks when the booting process is
 * behind it.
 *
 * ```typescript
 * const runIfLatestBuild = createDeployGate({ db })
 *
 * runIfLatestBuild(async () => { await reconcileIndexes() })
 * runIfLatestBuild(async () => { await runMigrations() })
 *
 * await runIfLatestBuild.run() // once, at the end of startup
 * ```
 *
 * ## Skip, not lock — a deliberate gap
 *
 * Two processes carrying the *identical* build timestamp both run the hooks;
 * nothing serialises them. That is by design. The problem being solved is
 * ordering (an old build must not overwrite a new one), not exclusion, and
 * registered hooks are required to be idempotent regardless — they run on
 * every boot of the newest build anyway. durabl has lease primitives that
 * could add exclusion, but a lock on the startup path is a new way to hang a
 * boot, so it is not taken by default.
 *
 * ## Build version
 *
 * The default version is the mtime of `process.argv[1]` (the server bundle):
 * cheap, monotonic per deploy, and needs no build-time codegen. That is the
 * fallback, not the contract — pass `buildTimestamp` explicitly to drive it
 * from a real build identity (image build time, CI timestamp), and pass
 * `revision` to record a git SHA or image digest alongside it for debugging.
 *
 * Prior art: `deathandmayhem/jolly-roger`, `imports/server/runIfLatestBuild.ts`.
 */
import { promises as fs } from 'node:fs'

import type { Collection, Db } from 'mongodb'

import { defaultLogger, type Logger } from './logger'

/** Singleton document recording the newest build observed so far. */
export interface DeploymentDoc {
  _id: string
  /** Build version of the newest process that has run its startup hooks. */
  buildTimestamp: Date
  /** Caller-supplied build identity (git SHA, image digest), if any. */
  revision?: string
  /** When the row was last moved forward. Diagnostics only. */
  updatedAt: Date
}

export interface DeployGateOptions {
  /** Database handle from a connected `MongoClient`. */
  db: Db
  /** Collection holding the singleton row. Default: `'deployments'`. */
  collectionName?: string
  /** `_id` of the singleton row. Default: `'default'`. */
  deploymentId?: string
  /**
   * Explicit build version. Wins over {@link DeployGateOptions.entrypoint};
   * supply it when you have a real build identity or are driving a test.
   */
  buildTimestamp?: Date
  /**
   * Path whose mtime is the build version, used when `buildTimestamp` is
   * omitted. Default: `process.argv[1]`.
   */
  entrypoint?: string
  /** Build identity recorded alongside the timestamp (git SHA, image digest). */
  revision?: string
  /** Injectable logger. Default: console. */
  logger?: Logger
}

export interface DeployGateResult {
  /** Did the hooks run? `false` means a newer build got here first. */
  ran: boolean
  /** The build version this process resolved for itself. */
  buildTimestamp: Date
  /** The version already on record, or `null` when this is the first boot. */
  previousTimestamp: Date | null
  /** How many registered hooks were executed. */
  hooksRun: number
}

export interface DeployGate {
  /**
   * Register a hook to run at {@link DeployGate.run}, but only if this process
   * is the newest build. Hooks run in registration order; registering the same
   * function reference twice registers it once.
   */
  (hook: () => Promise<void>): void
  /**
   * Resolve this process's build version, run the registered hooks if it is
   * the newest seen, then move the recorded version forward.
   *
   * A throwing hook propagates and the recorded version is left alone, so the
   * boot fails loudly rather than claiming a deploy that did not finish.
   */
  run(): Promise<DeployGateResult>
}

/** Mongo's unique-index violation, across driver/server error shapes. */
function isDuplicateKeyError(err: unknown): boolean {
  if (!(err instanceof Error)) return false
  const { code } = err as { code?: unknown }
  return code === 11000 || err.message.includes('E11000')
}

async function resolveBuildTimestamp(
  options: DeployGateOptions,
): Promise<Date> {
  if (options.buildTimestamp) return options.buildTimestamp

  const entrypoint = options.entrypoint ?? process.argv[1]
  if (!entrypoint) {
    throw new Error(
      'durabl deploy gate: no buildTimestamp given and process.argv[1] is ' +
        'empty — pass buildTimestamp or entrypoint explicitly.',
    )
  }
  const stat = await fs.stat(entrypoint)
  return stat.mtime
}

/**
 * Move the singleton row forward to `buildTimestamp`, never backward.
 *
 * The guarded update is the compare-and-swap. It matches nothing when the row
 * is absent *or* already at/ahead of us, so an absent row is handled by an
 * insert — and a losing insert re-runs the guarded update, because the process
 * that beat us to creation may have written an older timestamp than ours.
 */
async function recordLatestBuild(
  collection: Collection<DeploymentDoc>,
  deploymentId: string,
  buildTimestamp: Date,
  revision: string | undefined,
): Promise<void> {
  const fields = {
    buildTimestamp,
    updatedAt: new Date(),
    ...(revision === undefined ? {} : { revision }),
  }
  const advance = (): Promise<{ matchedCount: number }> =>
    collection.updateOne(
      { _id: deploymentId, buildTimestamp: { $lt: buildTimestamp } },
      { $set: fields },
    )

  const advanced = await advance()
  if (advanced.matchedCount > 0) return

  try {
    await collection.insertOne({ _id: deploymentId, ...fields })
  } catch (err) {
    if (!isDuplicateKeyError(err)) throw err
    await advance()
  }
}

/**
 * Build a {@link DeployGate} over `options.db`.
 *
 * The returned value is callable (register a hook) and carries `run()`.
 */
export function createDeployGate(options: DeployGateOptions): DeployGate {
  const collection = options.db.collection<DeploymentDoc>(
    options.collectionName ?? 'deployments',
  )
  const deploymentId = options.deploymentId ?? 'default'
  const logger = (options.logger ?? defaultLogger).child({
    category: 'DeployGate',
  })
  // A Set, so a module registering the same hook reference twice runs it once.
  const hooks = new Set<() => Promise<void>>()

  const gate = (hook: () => Promise<void>): void => {
    hooks.add(hook)
  }

  gate.run = async (): Promise<DeployGateResult> => {
    const buildTimestamp = await resolveBuildTimestamp(options)
    const previous = await collection.findOne({ _id: deploymentId })
    const previousTimestamp = previous?.buildTimestamp ?? null

    if (previousTimestamp && previousTimestamp > buildTimestamp) {
      logger.warn(
        {
          buildTimestamp,
          previousTimestamp,
          previousRevision: previous?.revision,
        },
        'Skipping startup hooks: a newer build has already run',
      )
      return { ran: false, buildTimestamp, previousTimestamp, hooksRun: 0 }
    }

    let hooksRun = 0
    for (const hook of hooks) {
      await hook()
      hooksRun += 1
    }

    await recordLatestBuild(
      collection,
      deploymentId,
      buildTimestamp,
      options.revision,
    )
    logger.info(
      { buildTimestamp, previousTimestamp, hooksRun },
      'Ran startup hooks as the latest build',
    )
    return { ran: true, buildTimestamp, previousTimestamp, hooksRun }
  }

  return gate
}

/**
 * One-shot convenience: run `hook` only if this process is the newest build.
 *
 * Equivalent to creating a gate, registering one hook and running it. Use
 * {@link createDeployGate} when several modules need to register.
 */
export async function runIfLatestBuild(
  options: DeployGateOptions,
  hook: () => Promise<void>,
): Promise<DeployGateResult> {
  const gate = createDeployGate(options)
  gate(hook)
  return gate.run()
}
