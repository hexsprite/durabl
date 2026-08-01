import { defineConfig } from 'tsup'

export default defineConfig({
  // Core entry + the opt-in ESLint plugin (durabl/eslint) and test harness
  // (durabl/testing), emitted separately so core consumers pull neither.
  entry: ['src/index.ts', 'src/eslint/index.ts', 'src/testing/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
  target: 'node18',
  // Peer / optional-peer deps — never bundle them.
  external: ['mongodb', 'eslint', '@typescript-eslint/utils'],
})
