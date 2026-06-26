# Releasing

Releases publish to PyPI via the **Release** GitHub Action
(`.github/workflows/release.yml`) using PyPI **Trusted Publishing** (OIDC) — no
API tokens are stored in the repo.

## One-time setup

On PyPI, add a Trusted Publisher for the `crdb-dump` project:

- Owner: `viragtripathi`
- Repository: `crdb-dump`
- Workflow: `release.yml`

## Cutting a release

1. Bump `version` in `pyproject.toml`.
2. In `CHANGELOG.md`, rename the `## Unreleased` section to `## <version> — <YYYY-MM-DD>`
   and add a fresh empty `## Unreleased` section above it.
3. Merge to `main`.
4. Run **Actions → Release → Run workflow** and enter the same version (e.g. `0.4.0`).

The workflow verifies the input matches the packaged version, runs the full test
suite against a CockroachDB container, builds the sdist/wheel, publishes to PyPI,
and creates a `v<version>` GitHub Release with auto-generated notes.
