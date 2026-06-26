# crdb-dump Documentation Site (Design)

Date: 2026-06-26
Status: Approved-pending-review

## Problem

`crdb-dump` has a large CLI surface (export/load/version with many options:
formats, chunking, gzip, ordering, parallelism, resume, validation, S3, region,
permissions, three-part naming, type handling). The README is straining to hold
all of it. The project should have a proper documentation website, consistent
with the CockroachDB ecosystem (e.g. `cockroachdb/langchain-cockroachdb`), and a
permanent home for guidance that currently only lives in conversation (notably:
this is a snapshot logical dump/restore tool, not a live-migration tool).

## Goals

1. A MkDocs (Material) documentation site mirroring the `langchain-cockroachdb` setup.
2. Auto-generated CLI reference that stays in sync with the Click app.
3. Auto-deploy to GitHub Pages on push to `main`.
4. A permanent "Migration & Limitations" page capturing what the tool is / isn't.

## Decisions (from brainstorming)

- **Tooling:** MkDocs + Material theme; mirror langchain's theme/extensions.
- **CLI reference:** auto-generated from the Click app via `mkdocs-click`
  (no `mkdocstrings` — crdb-dump is a CLI, not an importable library API).
- **Deploy:** GitHub Action on push to `main` → `mkdocs gh-deploy --force` to the
  `gh-pages` branch → https://viragtripathi.github.io/crdb-dump/.
- **Examples:** task-oriented recipe pages with copy-paste shell snippets (no
  separate runnable `examples/` folder).

## Tooling detail

- `mkdocs.yml` at repo root.
- Theme `material`: light (`default`) + dark (`slate`) palettes, indigo primary/accent;
  features `navigation.tabs`, `navigation.sections`, `navigation.expand`,
  `navigation.top`, `search.suggest`, `search.highlight`, `content.code.copy`,
  `content.code.annotate`.
- Plugins: `search`, `mkdocs-click`.
- Markdown extensions: `pymdownx.highlight` (anchor line numbers),
  `pymdownx.inlinehilite`, `pymdownx.snippets`, `pymdownx.superfences`,
  `pymdownx.tabbed` (alternate_style), `pymdownx.details`, `pymdownx.emoji`,
  `admonition`, `attr_list`, `md_in_html`, `toc` (permalinks).
- New `[project.optional-dependencies] docs` extra in `pyproject.toml`:
  `mkdocs-material`, `mkdocs-click`, `pymdown-extensions`.

## Site structure (`docs/`) and nav

```
Home                      docs/index.md
Getting Started
  Installation            docs/getting-started/installation.md
  Quick Start             docs/getting-started/quickstart.md
  Configuration           docs/getting-started/configuration.md
Guides
  Export Schema           docs/guides/export-schema.md
  Export Data             docs/guides/export-data.md
  Import & Restore        docs/guides/import-restore.md
  Multi-Schema Objects    docs/guides/multi-schema.md
  Type Handling           docs/guides/type-handling.md
  Permissions             docs/guides/permissions.md
  Region-Aware            docs/guides/region-aware.md
  S3-Compatible Storage   docs/guides/s3-storage.md
  Migration & Limitations docs/guides/migration-limitations.md
Recipes
  Full Dump & Restore     docs/recipes/full-dump-restore.md
  Selective Tables        docs/recipes/selective-tables.md
  Cross-Environment Copy  docs/recipes/cross-env-copy.md
  S3 Round-Trip           docs/recipes/s3-round-trip.md
  Verify & Resume         docs/recipes/verify-resume.md
CLI Reference
  Commands (auto)         docs/reference/cli.md         (mkdocs-click)
  Naming Model            docs/reference/naming-model.md
  Manifest Format         docs/reference/manifest-format.md
About
  Changelog               docs/about/changelog.md       (includes ../../CHANGELOG.md)
  License                 docs/about/license.md
Development
  Contributing            docs/development/contributing.md
  Testing                 docs/development/testing.md
  Releasing               docs/development/releasing.md
```

### CLI reference page

`docs/reference/cli.md` uses the mkdocs-click directive against the Click group:

```
::: mkdocs-click
    :module: crdb_dump.cli
    :command: main
```

This renders `export`, `load`, and `version` with all options from the live code.

### Migration & Limitations page

States plainly: crdb-dump is a point-in-time **logical** dump/restore tool. It is
**not** a live-migration tool — for minimal-downtime/continuous migration use
CockroachDB **MOLT** (Fetch + Replicator + Verify) or **LDR** (self-hosted
CRDB↔CRDB). Cloud tier moves (Basic→Standard→Advanced) follow the documented
export/import-via-storage path, which crdb-dump can automate **with a write-freeze
window**. Documents the consistency caveat: tables are read independently without
`AS OF SYSTEM TIME`, so a dump of a live database is not a transactionally
consistent snapshot across tables; links the AOST enhancement as future work.
Links to upstream CockroachDB migration docs.

## Hosting / CI

- `.github/workflows/docs.yml`:
  - Trigger: `push` to `main` on paths `docs/**`, `mkdocs.yml`, `crdb_dump/**`
    (CLI ref depends on source), plus `workflow_dispatch`.
  - Steps: checkout; setup Python 3.12; `pip install -e ".[docs]"`;
    `mkdocs gh-deploy --force` (publishes to the `gh-pages` branch).
  - Permissions: `contents: write` (to push `gh-pages`).
- One-time manual step: repo Settings → Pages → source = `gh-pages` branch.
- README: add a docs-site link and a docs badge.

## Verification

Docs-only change; the gate is:
- `mkdocs build --strict` passes locally (fails on broken internal links / nav
  references / missing files) — run before any push.
- `docs.yml` and existing workflow YAML parse.
- Existing unit + integration test suite stays green (unchanged).

No database or e2e run is required for the docs themselves.

## Implementation phases

1. `pyproject.toml` `docs` extra + `mkdocs.yml` skeleton (theme/plugins/nav) +
   `docs/index.md`; `mkdocs build --strict` green with placeholder-free Home.
2. Getting Started pages.
3. Guides pages (incl. Migration & Limitations).
4. Recipes pages.
5. CLI Reference (mkdocs-click) + Naming Model + Manifest Format.
6. About (Changelog/License) + Development (Contributing/Testing/Releasing).
7. `docs.yml` deploy workflow; README docs link/badge.
8. Final `mkdocs build --strict` + full test suite green; open PR.

Each phase ends with `mkdocs build --strict` passing.

## Out of scope

- Versioned docs (`mike`), internationalization, custom domain.
- Moving the repo to the `cockroachdb` org (URL would change to
  `cockroachdb.github.io/crdb-dump`); revisit if/when that happens.
