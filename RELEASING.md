# Releasing

`download-ticks` is published to crates.io by
[`.github/workflows/publish.yml`](.github/workflows/publish.yml) when a version
tag is pushed.

## One-time setup

Choose one authentication method.

### Trusted Publishing (recommended)

Trusted Publishing lets the workflow exchange a GitHub OIDC token for a
short-lived crates.io token, so no long-lived credential is stored in GitHub.

On crates.io, open the crate settings and add a trusted publisher with:

| field | value |
| --- | --- |
| repository | `raonagos/download-ticks` |
| workflow | `publish.yml` |
| environment | `crates-io` |

The workflow requests `id-token: write` and runs in the `crates-io`
environment, which must match the values above.

### API token

Create a crates.io API token scoped to this crate and add it as the GitHub
Actions secret `CARGO_REGISTRY_TOKEN`. The workflow uses the secret when it is
present and skips the OIDC exchange.

## Cut a release

1. Bump `version` in `Cargo.toml`.
2. Update the README or the changelog if the release needs it.
3. Commit and merge the change to `master`.
4. Create and push an annotated tag:

   ```sh
   git tag -a v1.8.0 -m "download-ticks 1.8.0"
   git push origin v1.8.0
   ```

5. Watch the `Publish` workflow. It checks that the tag matches `Cargo.toml`,
   runs formatting, clippy, tests, and packaging, then publishes to crates.io.
6. Confirm the release on <https://crates.io/crates/download-ticks> and
   <https://docs.rs/download-ticks>.

Tags must have the form `v<major>.<minor>.<patch>`. A tag that does not match
the version in `Cargo.toml` fails before publishing.

## Dry run

Run the workflow manually from the Actions tab with `dry_run` enabled. It runs
all checks and `cargo publish --dry-run` without uploading. The publish step is
also a dry run when the workflow is started this way.
