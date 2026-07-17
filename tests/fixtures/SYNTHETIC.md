# Synthetic fixture declaration

All files below `tests/fixtures/` are Hypertrial-generated synthetic examples,
not downloaded NASA observations or boundary data.

- `scripts/generate_netcdf_fixtures.py` creates the small TEMPO-shaped NetCDF
  file with invented coordinates, measurements, flags, and time.
- `scripts/generate_geo_fixtures.py` creates the small Parquet registries and
  weights from declared test rows without downloading geography.
- `cassettes/tempo_cmr_granules.json` is a hand-authored CMR-shaped record with
  an `example.test` URL and invented granule identifier.

Fixtures exist only to exercise schema and parser behavior. They contain no
credentials, signed URLs, complete catalogs, production geography, or live
payloads.
