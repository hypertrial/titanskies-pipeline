# Operators

Use this hub to run, validate, and maintain a local TitanSkies warehouse.
Schedules stay disabled until discovery, ingestion, and dbt builds are healthy.

## Path

1. **First run** — [Quickstart](../getting-started/index.md) (`make demo`).
2. **Choose a scope** — [Choose a scope](../getting-started/choose-a-scope.md)
   (NRT vs standard).
3. **Geography** — [Build geography artifacts](../getting-started/build-geography-artifacts.md).
4. **Live path** — [Run the pipeline](../guides/run-the-pipeline.md) and
   [Live readiness](../guides/live-readiness.md).
5. **Day-two** — [Day-two operations](../guides/day-two-operations.md).
6. **Recover** — [Validate and recover](../guides/validate-and-recover.md) and
   [Troubleshooting](../guides/troubleshooting.md).

## Credentials And Inputs

| Flow | Network / credentials | Operator-local inputs |
| --- | --- | --- |
| Offline demo | None | Synthetic geography |
| Live NRT / std | Earthdata Login (`~/.netrc` or `EARTHDATA_*`) | Production geography under `artifacts/geo` |
| Backfill | Earthdata Login | Chunked date windows |

Never commit `.env`, Earthdata credentials, NetCDF exports, live geography
artifacts, or DuckDB files. See
[Operator responsibilities](../concepts/operator-responsibilities.md).

## Confirm Success

After a successful demo or live run you should have marts under
`tempo_no2_marts` (and `tempo_no2_std_marts` when the standard scope is
enabled). Local checks verify technical shape; they are not health advice or
Hypertrial certification of data rights.
