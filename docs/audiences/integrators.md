# Integrators

Use this hub when another repository or offline tool consumes TitanSkies
outputs. Public marts are analytics inputs, not health, exposure, or
regulatory advice.

## Checklist

1. Consume `tempo_no2_marts` / `tempo_no2_std_marts` only. Do not treat ops,
   staging, or intermediate schemas as APIs.
2. Filter on `is_analysis_ready` for analysis-ready measurements.
3. Pin versions via
   [CHANGELOG.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/CHANGELOG.md);
   derived warehouses may require clean rebuilds between releases.
4. Export with DuckDB `COPY` or documented scripts; do not scrape intermediate
   tables.
5. There is no hosted TitanSkies API.

See [Integration](../concepts/integration.md) and
[Data contracts](../reference/data-contracts.md).
