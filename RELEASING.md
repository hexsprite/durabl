# Releasing

Releases are informal. There is no release branch and no release manager. You
cut a release when the CHANGELOG has enough in it to be worth telling consumers
about.

While the version is `0.x`, breaking changes ship in a **minor** bump and are
called out under `### BREAKING` in the CHANGELOG.

## Checklist

1. Run the gates locally:

   ```bash
   npm run typecheck && npm run lint && npm test && npm run build
   ```

2. Edit `CHANGELOG.md`. Move the `## [Unreleased]` entries under a new
   `## [X.Y.Z] - YYYY-MM-DD` heading. If anything breaks, add a `### BREAKING`
   section and an `### Upgrading from <prev>` checklist. Write the upgrade steps
   as actions a consumer can run, not as a description of the change.

   If the steps run long (code samples, a custom-backend contract change), put
   them in [`docs/migrating.md`](docs/migrating.md) instead and link to that
   section from the CHANGELOG. Keep the README focused on current usage: it
   documents how the package works today, not how to get here from an older
   version.

3. Update the link references at the bottom of the CHANGELOG: point
   `[unreleased]` at the new tag, and add a `[X.Y.Z]` compare link.

4. Bump the version without letting npm tag it:

   ```bash
   npm version X.Y.Z --no-git-tag-version
   ```

5. Commit, tag, push:

   ```bash
   git add CHANGELOG.md package.json package-lock.json
   git commit -m "chore: release X.Y.Z"
   git tag vX.Y.Z
   git push && git push --tags
   ```

The pushed tag runs `.github/workflows/publish.yml`, which re-runs every gate,
checks the tag against `package.json`, publishes to npm, and creates the GitHub
Release.

The release body is the CHANGELOG section for that version, copied verbatim. If
the version has no section, the workflow fails before publishing. So step 2 is
not optional: no changelog entry, no release.

## Publishing

The workflow uses npm **trusted publishing**: GitHub mints a short-lived OIDC
token and npm exchanges it for publish rights. There is no `NPM_TOKEN` secret to
rotate and no OTP prompt.

This requires a one-time setup on npmjs.com: on the `durabl` package page, under
**Settings → Trusted publisher**, add the GitHub repo `hexsprite/durabl` and the
workflow file `publish.yml`. Without it, `npm publish` in CI fails on auth.

If the workflow is broken and a release is urgent, publish by hand:

```bash
npm publish --otp="$(op item get Npmjs --otp)"
```

## Telling consumers

The CHANGELOG is the notification. For a release with BREAKING items, also open
a bead in the consuming repo naming the version and the exact changes it needs,
so the upgrade does not depend on someone reading the CHANGELOG at the right
moment.
