/**
 * Flat ESLint config.
 *
 * Two jobs:
 *  1. A baseline correctness gate over `src` and `test` (core `js` recommended,
 *     parsed as TypeScript). Rules the compiler already owns — `no-undef`,
 *     `no-unused-vars` — stay off; `tsc --noEmit` with `noUnusedLocals` /
 *     `noUnusedParameters` is the authority there, and duplicating them here
 *     just produces two diagnostics for one problem.
 *  2. Dogfooding `durabl/eslint`. The plugin this package publishes is wired up
 *     against this repo's own source, so a rule that misfires on real
 *     orchestrator code fails our lint before it reaches a consumer's.
 *
 * Written as `.ts` (resolved via jiti) so the plugin is imported straight from
 * `src/`, not from a build artifact — linting must not depend on `npm run build`
 * having been run first.
 */
import js from '@eslint/js'
import tsParser from '@typescript-eslint/parser'

import durabl from './src/eslint/index'

export default [
  {
    ignores: [
      'dist/**',
      'coverage/**',
      'node_modules/**',
      '.beads/**',
      // Harness-managed worktree checkouts; each lints in its own tree.
      '.claude/**',
    ],
  },
  js.configs.recommended,
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser: tsParser,
      ecmaVersion: 2022,
      sourceType: 'module',
    },
    rules: {
      // Owned by tsc (see header).
      'no-undef': 'off',
      'no-unused-vars': 'off',
      // `catch (err) { /* nothing */ }` is a real finding in a queue that
      // swallows handler errors for a living; deliberate no-ops must say so.
      'no-empty': ['error', { allowEmptyCatch: false }],
      // AGENTS.md: inject a Logger, never console.log. The two legitimate
      // exceptions (the console logger itself, and a skip notice in the
      // change-stream suite) already carry disable directives.
      'no-console': 'error',
    },
  },
  {
    files: ['src/**/*.ts', 'test/**/*.ts'],
    plugins: { durabl },
    rules: {
      'durabl/no-nondeterministic-control-path': 'error',
    },
  },
]
