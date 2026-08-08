/**
 * durabl — a Mongo-backed durable job queue.
 *
 * @example
 * ```typescript
 * import { MongoClient } from 'mongodb'
 * import { MongoJobQueue, JobQueue } from 'durabl'
 *
 * const client = await MongoClient.connect(process.env.MONGO_URL!)
 * const backend = new MongoJobQueue({ db: client.db('app') })
 * await backend.startup()
 *
 * const queue = new JobQueue(backend)
 *
 * queue.process<{ userId: string }>('welcome-email', async (job, ctx) => {
 *   await sendWelcomeEmail(job.data.userId)
 *   await ctx.complete()
 * })
 *
 * await queue.enqueue('welcome-email', { userId }, {
 *   dedupeKey: `welcome-email:${userId}`,
 * })
 * ```
 */

// Main classes
export {
  JobQueue,
  createJobQueue,
  getDefaultQueue,
  getGlobalBackend,
  setGlobalBackend,
} from './JobQueue'
export type { JobQueueOptions } from './JobQueue'

// Types
export type {
  AppendStepResult,
  CompleteClaimedResult,
  DedupeScope,
  EnqueueOptions,
  HeartbeatClaimedResult,
  Job,
  JobContext,
  JobDoc,
  JobEvent,
  JobEventSink,
  JobHandle,
  JobHandler,
  JobStatus,
  LifecycleWriteResult,
  ProcessorConfig,
  QueueStats,
  StepRecord,
} from './types'

// Durable-execution orchestrator (step-level resume)
export { Orchestrator } from './orchestrator/Orchestrator'
export type { StepJournalPort } from './orchestrator/context'
export type {
  OrchestratorConfig,
  OrchestratorContext,
  OrchestratorFn,
  StepKeys,
  StepOptions,
} from './orchestrator/types'
export {
  HeartbeatConfigConflict,
  isFatalOrchestrationError,
  JournalTooLarge,
  MaxDurationExceeded,
  NondeterminismError,
  NonRetryable,
  NonSerializableStepResult,
  OrchestrationUnsupportedError,
  OrchestratorTypeConflict,
  StepTimeout,
} from './journal/errors'

// Logger interface + console default (inject your own pino/winston instance)
export { type Logger, consoleLogger, defaultLogger } from './logger'

// Backend interface (for custom implementations)
export type { IJobQueueBackend } from './backends/IJobQueueBackend'

// Production backend
export { MongoJobQueue } from './backends/MongoJobQueue'
export type { MongoJobQueueOptions } from './backends/MongoJobQueue'
export { MongoChangeStreamWatcher } from './backends/MongoChangeStreamWatcher'

// Test backends
export { DummyBackend } from './backends/DummyBackend'
export { ImmediateBackend } from './backends/ImmediateBackend'
