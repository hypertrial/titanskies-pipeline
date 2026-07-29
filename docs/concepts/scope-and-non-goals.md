# Scope and non-goals

TitanSkies is MIT-licensed, local-first NASA TEMPO NO₂ and SWOT river warehouse
software.
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
- One explicit `riverpulse:events` lane for SWORD v17b reach topology and
  revision-safe Hydrocron Version D observations/discharges on bounded
  Sacramento, Rhine, and Murray corridors.

## What It Does Not Ship Or Operate

- No hosted dataset or API operated by Hypertrial.
- No health, personal-exposure, medical, safety, or regulatory advice.
- No flood warning, navigation, water-allocation, or emergency-response advice.
- No pixel-level history store; the public native-grid mart keeps latest cells
  only.
- No gauges/crosswalks, event detection, immutable event releases, nodes,
  direct RiverSP archives, GloFAS/ERA5, reservoir inventories, SDK/API,
  explorer, object storage, Iceberg/Delta, or PostGIS.
- No bundled production NetCDF, Hydrocron response, SWORD, network-generation,
  live geography, or DuckDB artifacts in the canonical repository.

## Operator Ownership

Every operator supplies Earthdata credentials when running live TEMPO
ingestion, optional Hydrocron API credentials when required, builds production
geography/network artifacts, and stores results in their own DuckDB file.

## Related Pages

- [Operator responsibilities](operator-responsibilities.md)
- [Legal and privacy](../reference/legal.md)
- [TEMPO product notes](tempo-product-notes.md)
- [RiverPulse product notes](riverpulse-product-notes.md)
