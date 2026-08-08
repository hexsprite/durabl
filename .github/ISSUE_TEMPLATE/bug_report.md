---
name: Bug report
about: Something behaves differently than documented
labels: bug
---

## What happened

## What you expected

## Environment

These three determine which code path is live, so please answer all of them:

- **durabl version:**
- **Backend:** MongoJobQueue / DummyBackend / ImmediateBackend
- **Mongo topology:** standalone or replica set (dedupe and lease behaviour are
  index-enforced, and change streams need a replica set)
- **Change streams:** `useChangeStreams` on or off (this changes the poll
  interval from 5s to the 60s safety net, which affects pickup latency)
- **Node version:**

## Reproduction

A failing test against `mongodb-memory-server` is the fastest possible bug
report — see CONTRIBUTING.md. If the bug involves dedupe, contention or leases,
try the chaos harness with a wider seed range first:

```bash
CHAOS_SEED=<n> CHAOS_RUNS=500 npx vitest run test/queueProperties.test.ts
```

## Relevant logs

durabl logs carry a `category` binding (`JobQueue`, `MongoJobQueue`,
`MongoChangeStreamWatcher`).
