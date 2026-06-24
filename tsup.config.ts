import { defineConfig } from 'tsup'

export default defineConfig({
  // Core entry + the opt-in ESLint plugin (durabl/eslint), emitted separately so
  // core consumers never pull the lint toolchain.
  entry: ['src/index.ts', 'src/eslint/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
  target: 'node18',
  // Peer / optional-peer deps — never bundle them.
  external: ['mongodb', 'eslint', '@typescript-eslint/utils'],
})
