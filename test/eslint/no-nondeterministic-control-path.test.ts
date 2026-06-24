import { RuleTester } from '@typescript-eslint/rule-tester'
import { afterAll, describe, it } from 'vitest'
import rule from '../../src/eslint/no-nondeterministic-control-path'

// Wire @typescript-eslint/rule-tester's hooks to vitest.
RuleTester.afterAll = afterAll
RuleTester.describe = describe
RuleTester.it = it
RuleTester.itOnly = it.only

const ruleTester = new RuleTester()

ruleTester.run('no-nondeterministic-control-path', rule, {
  valid: [
    // Branch only on a journaled step result + immutable job.data — the safe pattern.
    {
      code: `
        orch.define('restart-trial', async (job, octx) => {
          const existing = await octx.step('load', () => getSub(job.data.userId))
          if (existing && existing.active) {
            await octx.step('cancel', () => cancel(existing.id))
          }
          await octx.step('mark', () => mark(job.data.userId, octx.now()))
        })
      `,
    },
    // Fan-out: Promise.all over steps is allowed.
    {
      code: `
        orch.define('fan', async (job, octx) => {
          await Promise.all(job.data.ids.map((id) => octx.step('one:' + id, () => work(id))))
        })
      `,
    },
    // Awaits + nondeterminism INSIDE a step callback are fine (runs once, journaled).
    {
      code: `
        orch.define('inside', async (job, octx) => {
          await octx.step('io', async () => {
            const r = await fetch('https://api')
            const at = Date.now()
            return { r: await r.json(), at }
          })
        })
      `,
    },
    // Detected by 2nd-param type annotation, not the define() call.
    {
      code: `
        const fn = async (job: Job, octx: OrchestratorContext) => {
          await octx.step('a', () => load())
        }
      `,
    },
    // Destructured context: bare step()/heartbeat() calls allowed.
    {
      code: `
        orch.define('destructured', async (job, { step }) => {
          await step('a', () => load())
        })
      `,
    },
    // octx.now()/octx.uuid() are sync helpers — using them is the correct path.
    {
      code: `
        orch.define('clock', async (job, octx) => {
          const id = octx.uuid()
          await octx.step('use', () => persist(id, octx.now()))
        })
      `,
    },
    // Not an orchestrator (plain handler) — rule must not touch it.
    {
      code: `
        queue.process('email', async (job, ctx) => {
          await sendEmail(job.data.userId)
          await ctx.complete()
        })
      `,
    },
  ],

  invalid: [
    // The headline foot-gun: branch on a non-journaled live read.
    {
      code: `
        orch.define('flag', async (job, octx) => {
          if (await isFeatureEnabled('x')) {
            await octx.step('a', () => load())
          }
        })
      `,
      errors: [{ messageId: 'bareAwait' }],
    },
    // Bare external effect awaited in the body.
    {
      code: `
        orch.define('save', async (job, octx) => {
          await db.users.save(job.data)
        })
      `,
      errors: [{ messageId: 'bareAwait' }],
    },
    // Awaiting a helper (cross-boundary read) — strict rule flags it; wrap in a step.
    {
      code: `
        orch.define('helper', async (job, octx) => {
          const x = await decide(octx)
          if (x) await octx.step('a', () => load())
        })
      `,
      errors: [{ messageId: 'bareAwait' }],
    },
    // Direct Date.now() on the control path.
    {
      code: `
        orch.define('clock', async (job, octx) => {
          if (Date.now() > job.data.deadline) {
            await octx.step('a', () => load())
          }
        })
      `,
      errors: [{ messageId: 'syncNondet' }],
    },
    // Direct Math.random() on the control path.
    {
      code: `
        orch.define('rng', async (job, octx) => {
          const bucket = Math.random() < 0.5 ? 'a' : 'b'
          await octx.step('b:' + bucket, () => load())
        })
      `,
      errors: [{ messageId: 'syncNondet' }],
    },
    // new Date() (wall clock) captured on the control path, then fed to a step.
    {
      code: `
        orch.define('now', async (job, octx) => {
          const at = new Date()
          await octx.step('mark', () => mark(at))
        })
      `,
      errors: [{ messageId: 'syncNondet' }],
    },
    // randomUUID() directly instead of octx.uuid().
    {
      code: `
        orch.define('uuid', async (job, octx) => {
          const id = randomUUID()
          await octx.step('use', () => persist(id))
        })
      `,
      errors: [{ messageId: 'syncNondet' }],
    },
  ],
})
