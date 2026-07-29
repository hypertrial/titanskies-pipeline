# PlumeGraph product notes

PlumeGraph v0.6 is an auditable research evidence ledger for a frozen 2024
benchmark of 75 reviewed US power plants. It pins provisional TEMPO
`TEMPO_NO2_L2` V04 (`C3685896872-LARC_CLOUD`), hourly HRRR CONUS analysis at
forecast hour zero, EPA CAMD apportioned hourly NOx, eGRID 2023, and the
EPA–EIA crosswalk.

The tracked source manifest pins the canonical collection, archive, API,
metadata, and crosswalk contracts. Each acquired result records raw and
normalized checksums, schema fingerprints, and sanitized request/result
lineage. Production cohort and benchmark manifests require scientific-owner
approval; the repository ships only invented offline fixtures.

## Scientific contract

`dbt/seeds/plumegraph_events_contract.csv` is the sole policy source. Good
pixels require `main_data_quality_flag = 0`, cloud fraction below `0.1`,
finite VCD, positive uncertainty, valid WKB geometry, V04, and complete
provenance. Negative VCD remains usable for background estimation.

The baseline uses a median/MAD background from at least 30 pixels in the
wind-oriented 50–100 km upwind annulus. Seeds inside 60 km must exceed both
three MAD and twice combined uncertainty. Connected components require three
pixels, and tracking permits a three-hour cloud gap.
Seeded components below the three-pixel threshold and single-scan lineages are
retained with `insufficient_evidence`; they are not analysis-ready and do not
receive calibrated probabilities.

Candidate weights are trajectory `0.40`, concurrent CAMD NOx `0.30`, distance
`0.20`, and annual prior `0.10`. Missing hourly emissions prevent probability
readiness. A temperature is fitted on calibration facilities; held-out ECE
above `0.10` suppresses probabilities without discarding rank scores.

Integrated-mass output includes direct NO2-equivalent kg/h and every 10 m/80 m
and adjacent-hour wind, 2/4/6-hour lifetime, and 0.5/0.7/0.9 NO2:NOx
sensitivity. The 80 m, four-hour, 0.7 case is only the labeled central variant.

## Evidence limits

TEMPO V04 remains provisional. Retrieval, meteorology, background, geometry,
lifetime, and conversion uncertainty remain explicit. Attribution is
evidence—not proof, a health-risk conclusion, or a regulatory finding.
Scientific owners must approve the cohort, annotations, contract, and
held-out metrics before a public release.
