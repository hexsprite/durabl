/**
 * durabl/eslint — opt-in ESLint plugin for durabl Orchestrator authors.
 *
 * Ships separately from durabl core (which has no linter). Consumers who write
 * orchestrators add it to their EXISTING ESLint setup. `eslint` and
 * `@typescript-eslint/parser` are optional peers — the rule only needs the
 * TSESTree parser, not type information, so no `parserOptions.project` is required.
 *
 * Flat config:
 * ```js
 * import durabl from 'durabl/eslint'
 * export default [
 *   ...durabl.configs.recommended,
 *   // or wire the rule yourself:
 *   // { plugins: { durabl }, rules: { 'durabl/no-nondeterministic-control-path': 'error' } },
 * ]
 * ```
 */

import noNondeterministicControlPath from './no-nondeterministic-control-path'

const rules = {
  'no-nondeterministic-control-path': noNondeterministicControlPath,
}

const plugin = {
  meta: { name: 'durabl', version: '0.1.1' },
  rules,
  configs: {} as Record<string, unknown>,
}

// Flat-config preset (self-referential, so assigned after the object exists).
plugin.configs.recommended = [
  {
    plugins: { durabl: plugin },
    rules: {
      'durabl/no-nondeterministic-control-path': 'error',
    },
  },
]

export default plugin
export { noNondeterministicControlPath }
export type { Options, MessageIds } from './no-nondeterministic-control-path'
