# Contributing to TitanSkies Pipeline

TitanSkies accepts focused improvements to its local TEMPO ingestion,
orchestration, warehouse models, tests, and documentation. Do not attach
credentials, live NetCDF files, downloaded boundary data, or local DuckDB
warehouses to issues or pull requests.

## Development setup

```bash
uv sync --locked --extra dev --extra geo
uv run playwright install chromium
cp .env.example .env
python scripts/build_region_artifacts.py --synthetic
uv run make dbt-parse
```

Keep `TEMPO_NO2_HOURLY_PIPELINE_SCHEDULE_ENABLED=false` during development.
Use synthetic geography only for demos and tests. Live ingestion requires the
reviewed production build described in
[Build geography artifacts](docs/getting-started/build-geography-artifacts.md).

## Change expectations

- **Ingestion:** preserve the NASA product contract and add small synthetic or
  sanitized fixtures for new parsing behavior. Never record credentials or a
  full live response.
- **Geography:** pin source versions, keep the manifest/checksum contract, and
  validate all three supported countries. Production artifacts are generated
  locally and remain untracked.
- **dbt:** retain public grains and column meanings unless the change is
  explicitly documented as breaking. Update data contracts, the dictionary,
  and golden rows together.
- **Orchestration:** keep schedules opt-in and ensure registered jobs remain
  executable against disposable state.
- **Documentation:** run the strict MkDocs and local browser checks.

Version `0.3.x` does not migrate populated v0.1/v0.2 derived warehouses. Test
schema or geometry changes with a clean disposable database; do not add legacy
compatibility shims or copy old derived tables into a new warehouse.

## Validation

Use the smallest relevant target while iterating:

```bash
uv run make unit-ingest
uv run make unit-orchestration
uv run make integration-dbt
uv run make integration-dagster
uv run make contract-http
uv run make docs-check
```

Before opening a pull request, run the complete gate in [AGENTS.md](AGENTS.md).
The suite requires 100% branch coverage. Install the pinned Costguard release
described in the development guide before running its final target.

## Pull requests and security

Keep each pull request independently reviewable, explain warehouse or data
contract impact, and list the commands actually run. Update README or docs for
operator-visible changes and add an Unreleased changelog entry. Use GitHub
issues for normal bugs; follow [SECURITY.md](SECURITY.md) for vulnerabilities.

## Developer Certificate of Origin

Every commit must be signed off under the
[Developer Certificate of Origin 1.1](https://developercertificate.org/) with:

```text
Signed-off-by: Your Name <your-email@example.com>
```

Use `git commit -s` to add the sign-off. By signing off, you certify that you
have the right to submit the contribution and license it under this
repository's MIT licence. Hypertrial does not require a contributor licence
agreement.

Your name, sign-off, commit email, GitHub account, and submitted contribution
metadata become public and may be retained indefinitely. GitHub's no-reply
email option is recommended if you do not want a personal email address in
public history. Pull requests must pass the required DCO status check.

See [PRIVACY.md](PRIVACY.md) for contributor-data handling and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) before adding source-derived
material.
