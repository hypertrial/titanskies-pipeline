# TitanSkies Pipeline

TitanSkies is a local-first NASA TEMPO NO₂ warehouse with administrative
history and native-grid latest observations for Canada, the United States,
and Mexico. It publishes analyst-ready DuckDB marts without operating a hosted
dataset or API.

TitanSkies ships two parallel scopes from the same warehouse: `tempo:no2`
(near-real-time) and `tempo:no2_std` (standard, V04). See
[Orchestration](reference/orchestration.md) for the per-scope jobs and
schedules.

## Start here

| Goal | Guide |
| --- | --- |
| Build a credential-free warehouse | [Quickstart](getting-started/index.md) |
| Run discovery and ingestion | [Run the pipeline](guides/run-the-pipeline.md) |
| Query or export analyst-ready data | [Query the warehouse](guides/query-the-warehouse.md) |
| Understand tables and ownership | [Warehouse reference](reference/warehouse.md) and [Data dictionary](reference/data-dictionary.md) |
| Configure live operation | [Configuration](reference/configuration.md) and [Live readiness](guides/live-readiness.md) |
| Diagnose and recover a run | [Troubleshooting](guides/troubleshooting.md) |
| Change or validate the project | [Development guide](development/index.md) |

Analysts should query `tempo_no2_marts` and filter measurements on
`is_analysis_ready`. Operators should begin investigations in
`tempo_no2_observability` and the durable `tempo_no2_ops` ledger.

TitanSkies is research and engineering software, not health, exposure,
medical, safety, or regulatory advice. It has no telemetry and sends no user
data to Hypertrial. Review the
[source notices](https://github.com/hypertrial/titanskies-pipeline/blob/main/THIRD_PARTY_NOTICES.md)
and [privacy notice](https://github.com/hypertrial/titanskies-pipeline/blob/main/PRIVACY.md).
