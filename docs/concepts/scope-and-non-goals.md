# Scope and non-goals

TitanSkies is MIT-licensed, local-first NASA TEMPO NO₂ warehouse software.
This page is the short human summary. The authoritative licence and
third-party boundary is
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
For the operator checklist, see
[Operator responsibilities](operator-responsibilities.md).

## What This Repository Ships

- Source code, dbt models, Dagster jobs, operator scripts, and documentation.
- Two parallel scopes: `tempo:no2` (NRT) and `tempo:no2_std` (standard V04).
- Administrative history and native-grid latest observations for Canada, the
  United States, and Mexico.

## What It Does Not Ship Or Operate

- No hosted dataset or API operated by Hypertrial.
- No health, personal-exposure, medical, safety, or regulatory advice.
- No pixel-level history store; the public native-grid mart keeps latest cells
  only.
- No bundled production NetCDF or live geography artifacts in the canonical
  repository.

## Operator Ownership

Every operator supplies Earthdata credentials when running live ingestion,
builds geography artifacts, and stores results in their own DuckDB file.

## Related Pages

- [Operator responsibilities](operator-responsibilities.md)
- [Legal and privacy](../reference/legal.md)
- [TEMPO product notes](tempo-product-notes.md)
