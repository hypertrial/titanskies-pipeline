# Releasing

This page is the operator checklist for cutting a TitanSkies release. GitHub
Actions only runs the five-minute offline fast gate; the full release gate
must pass on a maintainer machine before tagging.

## Local release gate

From a clean checkout on `main`, sync dependencies and run the gate in order:

```bash
uv sync --locked --extra dev --extra geo
uv run make lint
uv run make test-cov
uv run make dagster-jobs-smoke-cov
uv run make dagster-refresh-cov
uv run make integration-dbt-cov
uv run make dbt-unit
uv run make golden-dbt
uv run make dbt-source-freshness-ci
uv run make coverage-report
uv run make docs-check
uv run make check-secrets
uv run make dbt-parse
uv run make dbt-build-ci
uv run make gx-data-quality
uv run make costguard
```

Do not create a tag until every target is green and the accumulated coverage
report remains at 100% branch coverage. The local Costguard gate uses the
pinned `2.5.0` release; install it with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

## Cut the release

1. Date the Unreleased section in `CHANGELOG.md` and set
   `pyproject.toml` to the same version.
2. Commit the release notes and any docs updates on `main`, then push.
3. Confirm the GitHub Actions fast gate is green on that commit.
4. Tag and push the release:

   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

5. Publish the GitHub Release from the dated changelog section:

   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z" --notes-file /tmp/release-notes.md
   ```

6. Deploy documentation from the tagged commit:

   ```bash
   uv run mkdocs gh-deploy --force
   ```

   Docs publish locally by design. Do not add a docs workflow under
   `.github/workflows/`; the repository policy keeps a single offline CI
   runner.

## Post-release checks

- Confirm CI is green on `main` and on the release tag.
- Confirm `https://hypertrial.github.io/titanskies-pipeline/` returns HTTP
  200 and renders the homepage.
- Review open Dependabot pull requests and merge compatible updates before
  the next release cycle.

## Dependency pin watchlist

- Keep `dbt-core` constrained below `1.12` until a stable `dagster-dbt`
  release supports that boundary, then remove the pin and Dependabot ignore.
- Retain the `netCDF4` 1.7.3 / 1.7.4 split while uv needs 1.7.3 for Python
  3.10 on Windows ARM64; do not treat 1.7.4 as universal.
