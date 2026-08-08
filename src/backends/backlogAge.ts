/** Shared backlog-age computation for the in-memory backends. */
/**
 * Backlog age over an in-memory job list: the `runAt` of the oldest pending job
 * that is already due, plus how far past due it is.
 *
 * Future-dated jobs are excluded for the same reason as in the Mongo backend —
 * a job scheduled for next week is not late, and counting it would peg the
 * metric permanently red.
 */
export function backlogAge(
  jobs: Array<{ status: string; runAt: Date }>,
  now: Date = new Date(),
): { oldestPendingRunAt: Date | null; oldestPendingLagMs: number } {
  let oldest: Date | null = null
  for (const job of jobs) {
    if (job.status !== 'pending') continue
    if (job.runAt.getTime() > now.getTime()) continue
    if (oldest === null || job.runAt.getTime() < oldest.getTime()) {
      oldest = job.runAt
    }
  }
  return {
    oldestPendingRunAt: oldest,
    oldestPendingLagMs: oldest ? Math.max(0, now.getTime() - oldest.getTime()) : 0,
  }
}
