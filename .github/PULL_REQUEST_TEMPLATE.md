## What and why

## Gates

- [ ] `npm run typecheck`
- [ ] `npm run lint`
- [ ] `npm test`
- [ ] `npm run build`

## Checklist

- [ ] Bug fix? A regression test that fails against the old code and passes
      against the new one, named in behavioural terms.
- [ ] Touched the claim query, its sort, or a dedupe index? Say why it is still
      atomic, and keep the `explain()` assertions passing — the index key order
      is load-bearing for a measured reason.
- [ ] Changed backend behaviour? All backends still agree. An in-memory backend
      more permissive than Mongo makes the test suite certify a lie.
- [ ] Public surface changed? README and CHANGELOG updated in this PR.
