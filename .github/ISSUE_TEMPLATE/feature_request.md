---
name: Feature request
about: Propose a change to the public surface
labels: enhancement
---

## The problem

What are you unable to do today? Please describe the situation rather than the
solution — the shape of the fix often changes once the constraint is clear.

## What you tried

Especially: does `claimOrEnqueue` + `dedupeScope: 'pending'` already cover it?
It replaces a distributed lock for the run-now-never-twice case, and that is not
obvious from the outside.

## Scope

- Does this need a new backend method, or can it live in `JobQueue`? Anything in
  `JobQueue` works for every backend for free.
- Is it a breaking change to the public surface?
