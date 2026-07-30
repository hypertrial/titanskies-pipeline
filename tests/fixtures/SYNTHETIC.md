# Synthetic fixture declaration

All files below `tests/fixtures/` are Hypertrial-generated synthetic examples,
not downloaded NASA observations or boundary data.

- `scripts/generate_netcdf_fixtures.py` creates the small TEMPO-shaped NetCDF
  file with invented coordinates, measurements, flags, and time.
- `scripts/generate_geo_fixtures.py` creates the small Parquet registries and
  weights from declared test rows without downloading geography.
- `cassettes/tempo_cmr_granules.json` is a hand-authored CMR-shaped record with
  an `example.test` URL and invented granule identifier.
- `cassettes/riverpulse_hydrocron.csv` is a hand-authored Hydrocron-shaped
  response with invented reach/granule IDs, two revisions of one observation,
  and an `example.test` provenance value.
- `cassettes/plumegraph_tempo_pixels.csv` is an invented TEMPO L2-shaped scene
  with plume, background, and rejected pixels.
- `cassettes/plumegraph_hrrr_analysis.csv` contains invented bracketing HRRR
  winds and scalar meteorology.
- `cassettes/plumegraph_camd_hourly.csv` contains invented CAMD-shaped
  facility/unit emissions with `example.test` provenance.
- `reproductions/sun2025_preflight.json` and
  `reproductions/andreadis2025_preflight.json` are hand-authored
  metadata-only source inventories with invented object IDs, sizes, and
  `example.test` URLs. They do not establish production-source availability.
- `reproductions/readiness_catalogs.json` contains invented CMR, Zenodo, EPA,
  GEOS-CF, CDS-import, SWOT L4, and Git metadata responses for resolver replay.
- `scripts/build_riverpulse_network.py --synthetic` creates invented
  Sacramento/Rhine/Murray mini-corridors without downloading SWORD.

Fixtures exist only to exercise schema and parser behavior. They contain no
credentials, signed URLs, complete catalogs, production geography, or live
payloads.
