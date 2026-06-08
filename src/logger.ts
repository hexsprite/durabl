/**
 * Minimal structured-logger interface.
 *
 * durabl logs in the pino convention: an optional context object as the
 * first argument, then a message string. Pass your own pino/winston/bunyan
 * instance (they all satisfy this shape) or rely on the bundled
 * {@link consoleLogger} default.
 */
export interface Logger {
  debug(objOrMsg: unknown, msg?: string): void
  info(objOrMsg: unknown, msg?: string): void
  warn(objOrMsg: unknown, msg?: string): void
  error(objOrMsg: unknown, msg?: string): void
  /** Return a child logger with extra bindings merged into every line. */
  child(bindings: Record<string, unknown>): Logger
}

type Level = 'debug' | 'info' | 'warn' | 'error'

function emit(
  level: Level,
  bindings: Record<string, unknown>,
  objOrMsg: unknown,
  msg?: string,
): void {
  // Normalise the pino-style (obj, msg) / (msg) overloads into one shape.
  const hasContext = msg !== undefined || typeof objOrMsg === 'object'
  const message = msg ?? (typeof objOrMsg === 'string' ? objOrMsg : '')
  const context =
    hasContext && typeof objOrMsg === 'object' && objOrMsg !== null
      ? { ...bindings, ...(objOrMsg as Record<string, unknown>) }
      : bindings
  const line = Object.keys(context).length
    ? [message, context]
    : [message]
  // eslint-disable-next-line no-console
  console[level](...line)
}

/** Build a {@link Logger} that writes to the console. */
export function consoleLogger(
  bindings: Record<string, unknown> = {},
): Logger {
  return {
    debug: (o, m) => emit('debug', bindings, o, m),
    info: (o, m) => emit('info', bindings, o, m),
    warn: (o, m) => emit('warn', bindings, o, m),
    error: (o, m) => emit('error', bindings, o, m),
    child: (extra) => consoleLogger({ ...bindings, ...extra }),
  }
}

/** Default logger used when no logger is injected. */
export const defaultLogger: Logger = consoleLogger()
