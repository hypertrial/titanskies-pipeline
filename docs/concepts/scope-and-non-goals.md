# Scope and non-goals

TitanSkies is MIT-licensed, local-first NASA TEMPO NO₂, SWOT river, and
power-plant plume-evidence warehouse software.
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
- One explicit `plumegraph:events` lane for a reviewed 2024 US power-plant
  cohort, TEMPO L2/HRRR/CAMD revisions, auditable plume episodes, validation,
  and immutable local evidence releases.
- Two unscheduled research profiles that pin and preflight the source inputs
  needed to reproduce DOI `10.1029/2025JD044565` and DOI
  `10.1029/2024GL114185`.

## What It Does Not Ship Or Operate

- No hosted dataset or API operated by Hypertrial.
- No health, personal-exposure, medical, safety, or regulatory advice.
- No flood warning, navigation, water-allocation, or emergency-response advice.
- No pixel-level history store; the public native-grid mart keeps latest cells
  only.
- No RiverPulse gauges/crosswalks, event detection/releases, or nodes;
  no PlumeGraph HCHO, TROPOMI, STAC/COG, health-risk, or enforcement claims;
  no direct RiverSP archives, GloFAS/ERA5, reservoir inventories, SDK/API,
  explorer, object storage, Iceberg/Delta, or PostGIS.
- No bundled production NetCDF, HRRR, EPA, benchmark-output, PlumeGraph
  release, Hydrocron response, SWORD, network-generation, live geography, or
  DuckDB artifacts in the canonical repository.
- No claim that the papers have been reproduced. v0.7 preflight validates a
  discovery inventory; full provider acquisition, normalization, analysis,
  and result comparison remain follow-on work.

## Operator Ownership

Every operator supplies Earthdata credentials when running live TEMPO
ingestion, optional Hydrocron API credentials when required, builds production
geography/network artifacts, supplies an approved PlumeGraph cohort and expert
benchmark, and stores results in their own DuckDB file.

## Related Pages

- [Operator responsibilities](operator-responsibilities.md)
- [Legal and privacy](../reference/legal.md)
- [TEMPO product notes](tempo-product-notes.md)
- [RiverPulse product notes](riverpulse-product-notes.md)
- [PlumeGraph product notes](plumegraph-product-notes.md)
- [Paper reproduction preflight](paper-reproduction-preflight.md)
