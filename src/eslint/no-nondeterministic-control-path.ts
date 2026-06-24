/**
 * ESLint rule: no-nondeterministic-control-path
 *
 * Enforces the durabl Orchestrator determinism rule (orchestrator-spec.md §2.1):
 * the orchestrator control path must be a pure function of `(job.data + journaled
 * step results)`. Resume-from-step re-runs the orchestrator body top-to-bottom on
 * every resume, so any nondeterministic value the control flow branches on can
 * route a resume to a different `step()` and corrupt the journal.
 *
 * The rule is deliberately STRICT and structural: inside an orchestrator body the
 * only thing you may `await` is `octx.*` (the context) or a `Promise` combinator
 * over those, and you may not call `Date.now()` / `Math.random()` / `new Date()` /
 * `randomUUID()` directly. Anything external belongs inside `octx.step(...)`, where
 * it runs exactly once and is journaled. The strictness is the point — it makes the
 * control path trivially auditable as pure.
 *
 * KNOWN CEILING (documented, not a bug): this is a LOCAL AST lint. It cannot follow
 * a nondeterministic read hidden one function call away
 * (`if (await myHelper())` where `myHelper` does the live read). Whole-program taint
 * or a replay sandbox would be required to catch that — both are explicitly out of
 * scope for resume-from-step. Divergence detection (spec §6) + review are the
 * backstop for what the lint cannot see.
 */

import { ESLintUtils, TSESTree } from '@typescript-eslint/utils'

const T = TSESTree.AST_NODE_TYPES

export type Options = [
  {
    /** Type names that mark a function's 2nd param as an orchestrator context. */
    contextTypeNames?: string[]
    /** Method names treated as orchestrator registration (`orch.define(...)`). */
    defineNames?: string[]
    /** Method/function names whose callback body is a step (awaits inside are fine). */
    stepNames?: string[]
  },
]

export type MessageIds = 'bareAwait' | 'syncNondet'

const PROMISE_COMBINATORS = new Set(['all', 'allSettled', 'race', 'any'])

const createRule = ESLintUtils.RuleCreator(
  () =>
    'https://github.com/hexsprite/durabl/blob/main/docs/orchestrator-spec.md#22-lint-enforcement-no-nondeterministic-control-path',
)

type Fn = TSESTree.ArrowFunctionExpression | TSESTree.FunctionExpression

function isFn(n: TSESTree.Node): n is Fn {
  return (
    n.type === T.ArrowFunctionExpression || n.type === T.FunctionExpression
  )
}

/** Context binding shape: member-style ids (`octx.step`) + destructured bare names (`step`). */
interface Ctx {
  ids: string[]
  members: string[]
}

export default createRule<Options, MessageIds>({
  name: 'no-nondeterministic-control-path',
  meta: {
    type: 'problem',
    docs: {
      description:
        'Disallow nondeterministic values on a durabl Orchestrator control path; route external reads through octx.step().',
    },
    schema: [
      {
        type: 'object',
        additionalProperties: false,
        properties: {
          contextTypeNames: { type: 'array', items: { type: 'string' } },
          defineNames: { type: 'array', items: { type: 'string' } },
          stepNames: { type: 'array', items: { type: 'string' } },
        },
      },
    ],
    messages: {
      bareAwait:
        'Orchestrator bodies may only `await` the context (`{{ctx}}.*`) or a Promise combinator over steps. Wrap external reads/effects in `{{ctx}}.step()` so they run once and are journaled, not re-run on resume.',
      syncNondet:
        '`{{call}}` is nondeterministic on the orchestrator control path; resume would compute a different value. Use `{{ctx}}.now()` / `{{ctx}}.uuid()`, or wrap it in `{{ctx}}.step()`.',
    },
  },
  defaultOptions: [{}],
  create(context, [opts]) {
    const contextTypeNames = opts.contextTypeNames ?? ['OrchestratorContext']
    const defineNames = opts.defineNames ?? ['define']
    const stepNames = opts.stepNames ?? ['step']
    const sourceCode = context.sourceCode

    /** Is `fn` an orchestrator body? Returns its context binding, or null. */
    function orchestratorCtx(fn: Fn): Ctx | null {
      const param = fn.params[1]
      if (!param) return null

      let matched = false

      // a) 2nd param typed as OrchestratorContext
      const annotated =
        param.type === T.Identifier ? param.typeAnnotation : undefined
      const typeAnn = annotated?.typeAnnotation
      if (
        typeAnn?.type === T.TSTypeReference &&
        typeAnn.typeName.type === T.Identifier &&
        contextTypeNames.includes(typeAnn.typeName.name)
      ) {
        matched = true
      }

      // b) fn is arg[1] of a `*.define(type, fn)` call
      const parent = fn.parent
      if (
        parent?.type === T.CallExpression &&
        parent.arguments[1] === fn &&
        parent.callee.type === T.MemberExpression &&
        parent.callee.property.type === T.Identifier &&
        defineNames.includes(parent.callee.property.name)
      ) {
        matched = true
      }

      if (!matched) return null

      if (param.type === T.Identifier) return { ids: [param.name], members: [] }
      if (param.type === T.ObjectPattern) {
        const members: string[] = []
        for (const prop of param.properties) {
          if (
            prop.type === T.Property &&
            prop.value.type === T.Identifier
          ) {
            members.push(prop.value.name)
          }
        }
        return { ids: [], members }
      }
      return { ids: [], members: [] }
    }

    /** Is `fn` the callback argument of a `*.step(...)` / `step(...)` call? */
    function isStepCallback(fn: Fn): boolean {
      const parent = fn.parent
      if (parent?.type !== T.CallExpression) return false
      if (!parent.arguments.includes(fn)) return false
      const callee = parent.callee
      if (
        callee.type === T.MemberExpression &&
        callee.property.type === T.Identifier &&
        stepNames.includes(callee.property.name)
      ) {
        return true
      }
      return callee.type === T.Identifier && stepNames.includes(callee.name)
    }

    /**
     * Walk ancestors to the nearest enclosing function and classify it.
     * Returns the orchestrator ctx to enforce against, or null (step callback,
     * plain helper, or top level — all out of enforcement scope here).
     */
    function enclosingOrchestrator(node: TSESTree.Node): Ctx | null {
      const ancestors = sourceCode.getAncestors(node)
      for (let i = ancestors.length - 1; i >= 0; i--) {
        const a = ancestors[i]
        if (isFn(a)) {
          if (isStepCallback(a)) return null // awaits/nondet inside a step body are fine
          return orchestratorCtx(a) // ctx if orchestrator body; null if a plain helper
        }
      }
      return null
    }

    function allowedAwaitArg(arg: TSESTree.Expression, ctx: Ctx): boolean {
      if (arg.type !== T.CallExpression) return false
      const callee = arg.callee
      if (callee.type === T.MemberExpression && callee.object.type === T.Identifier) {
        if (ctx.ids.includes(callee.object.name)) return true
        if (
          callee.object.name === 'Promise' &&
          callee.property.type === T.Identifier &&
          PROMISE_COMBINATORS.has(callee.property.name)
        ) {
          return true
        }
      }
      return callee.type === T.Identifier && ctx.members.includes(callee.name)
    }

    function ctxLabel(ctx: Ctx): string {
      return ctx.ids[0] ?? 'octx'
    }

    return {
      AwaitExpression(node) {
        const ctx = enclosingOrchestrator(node)
        if (!ctx) return
        if (allowedAwaitArg(node.argument, ctx)) return
        context.report({
          node,
          messageId: 'bareAwait',
          data: { ctx: ctxLabel(ctx) },
        })
      },

      CallExpression(node) {
        // Date.now() / Math.random() / crypto.randomUUID() / randomUUID()
        const callee = node.callee
        let label: string | null = null
        if (
          callee.type === T.MemberExpression &&
          callee.object.type === T.Identifier &&
          callee.property.type === T.Identifier
        ) {
          const m = `${callee.object.name}.${callee.property.name}`
          if (m === 'Date.now' || m === 'Math.random' || m === 'crypto.randomUUID') {
            label = `${m}()`
          }
        } else if (callee.type === T.Identifier && callee.name === 'randomUUID') {
          label = 'randomUUID()'
        }
        if (!label) return
        const ctx = enclosingOrchestrator(node)
        if (!ctx) return
        context.report({
          node,
          messageId: 'syncNondet',
          data: { call: label, ctx: ctxLabel(ctx) },
        })
      },

      NewExpression(node) {
        // new Date() with no args = wall clock; new Date(deterministicMs) is fine.
        if (
          node.callee.type !== T.Identifier ||
          node.callee.name !== 'Date' ||
          node.arguments.length > 0
        ) {
          return
        }
        const ctx = enclosingOrchestrator(node)
        if (!ctx) return
        context.report({
          node,
          messageId: 'syncNondet',
          data: { call: 'new Date()', ctx: ctxLabel(ctx) },
        })
      },
    }
  },
})
