/**
 * Minimal end-to-end durabl example. Run it directly:
 *
 *   MONGO_URL="mongodb://localhost:27017/?replicaSet=rs0" node examples/quickstart.mjs
 *
 * Shows the whole loop: connect, register a processor, enqueue, drain, exit.
 * Deliberately small — the interesting parts are the two lines most first-time
 * users leave out, marked below.
 */
import { MongoClient } from 'mongodb'
import { JobQueue, MongoJobQueue } from 'durabl'

const url = process.env.MONGO_URL
if (!url) {
  console.error('set MONGO_URL to a Mongo connection string')
  process.exit(1)
}

const client = await MongoClient.connect(url)
const backend = new MongoJobQueue({
  db: client.db('durabl_example'),
  collectionName: 'example_jobs',
})
await backend.startup() // creates indexes

const queue = new JobQueue(backend, undefined, {
  // Optional: lifecycle events for metrics. Logs are for humans, this is for
  // counters.
  onJobEvent: (e) => console.log('[event]', e.kind, e.jobId ?? ''),
})

// (1) Run the reaper somewhere. Without it, a job whose worker dies stays
//     `active` forever — and if it holds a dedupeKey, it blocks every future
//     job for that key. One process is enough.
queue.startReaper()

// (2) Drain on SIGTERM. Without it, a deploy kills in-flight handlers, they sit
//     active until the visibility timeout, burn an attempt, and re-run their
//     side effects from the top.
queue.installSignalHandlers({ timeoutMs: 10_000 })

queue.process(
  'greet',
  async (job, ctx) => {
    console.log(`hello, ${job.data.name}`)
    await ctx.complete() // required — see README
  },
  { concurrency: 2 },
)

// dedupeKey makes this idempotent: run the script twice, one greeting per name
// while the first is still pending.
await queue.enqueue('greet', { name: 'world' }, { dedupeKey: 'greet:world' })
await queue.enqueue('greet', { name: 'again' }, { dedupeKey: 'greet:again' })

// Give the processor a moment, then report backlog age — the metric worth
// alerting on, because depth alone cannot tell healthy from stuck.
await new Promise((r) => setTimeout(r, 500))
console.log('stats:', await queue.getStats('greet'))

await queue.shutdown()
await client.close()
