/** A no-op logger so expected error/warn paths don't spam test output. */
import type { Logger } from '../src/logger'

export const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
  child: () => silentLogger,
}
