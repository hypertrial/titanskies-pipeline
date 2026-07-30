# Operator responsibilities

This page is the operational checklist for legal and distribution hygiene.
The authoritative licence and third-party boundary remains
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/THIRD_PARTY_NOTICES.md).

## Authority

TitanSkies is MIT-licensed software and documentation. The MIT grant covers
Hypertrial-authored code and docs. It does **not** grant rights in NASA TEMPO
data, geography sources, or derived outputs an operator obtains or generates.

## Operator Checklist

- Confirm you are authorized to use Earthdata Login and TEMPO products.
- Keep `.env`, NetCDF downloads, live geography artifacts, and DuckDB files
  operator-local and untracked.
- Treat redistribution of warehouses and exports as your responsibility under
  applicable source terms.
- Run live readiness checks only on an operator-owned machine; GitHub Actions
  never downloads live NetCDF or production geography.
- Keep high-cardinality reproduction inventories, CDS/provider exports, GRDC
  inputs, credentials, and signed URLs untracked. A definitive source blocker
  may be replaced only by new immutable evidence, never by relabeling a
  fallback.

## Not Advice

TitanSkies is research and engineering software. Near-real-time observations
are provisional and are not measurements of an individual's exposure. NASA and
geography providers do not endorse TitanSkies or Hypertrial.

## Privacy And Telemetry

The software has no telemetry and sends no user, warehouse, measurement, or
credential data to Hypertrial. See
[PRIVACY.md](https://github.com/hypertrial/titanskies-pipeline/blob/main/PRIVACY.md).
