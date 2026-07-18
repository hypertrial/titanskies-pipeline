# TEMPO product notes

Product: `TEMPO_NO2_L3_NRT`

Version: `V02`

CMR concept ID: `C3685668637-LARC_CLOUD`

Collection DOI: `10.5067/IS-40e/TEMPO/NO2_NRT_L3.002`

Measure: tropospheric NO₂ vertical column

Format: NetCDF4 with a `product` group for science variables and root-level 1D `latitude` / `longitude` coordinates on the operational grid.

Ingestion requires deterministic v0.3 overlap weights and rejects incompatible
NetCDF coordinates. It never substitutes approximate bounding-box assignment.

Expected latency: under two to three hours

The current authoritative CMR metadata identifies the collection as
`TEMPO_NO2_L3_NRT_V02`, provisional, and near-real-time. Cite the collection
and DOI as documented in the
[source notices](https://github.com/hypertrial/titanskies-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
The ASDC citation page and CMR metadata must be rechecked for each release; a
DOI must never be inferred.

TitanSkies is for research and engineering use. It is not health, exposure,
medical, safety, or regulatory advice, and gridded/administrative aggregates
are not measurements of personal exposure.

## Standard (V04) collection

Product: `TEMPO_NO2_L3`

Version: `V04`

CMR concept ID: `C3685896708-LARC_CLOUD`

Collection DOI: `10.5067/IS-40E/TEMPO/NO2_L3.004`

The standard product is a separate, independently versioned TEMPO Level 3
collection ("TEMPO Standard V04", currently PROVISIONAL per the ASDC release
notes) rather than a "validated" replacement for NRT. It is derived from the
same East-West scan-cycle Level 2 inputs and re-gridding algorithm as the NRT
collection, and revises/settles more slowly, so TitanSkies runs standard
discovery on a wider 24-hour default lookback window
(`TEMPO_NO2_STD_DISCOVERY_LOOKBACK_HOURS`) versus 8 hours for NRT.

NetCDF variable names, group layout (`product` group plus root-level 1D
`latitude`/`longitude`), and the v0.3 overlap-weight ingestion contract are
assumed unchanged between the NRT and standard V04 products; TitanSkies
reuses the same NetCDF extraction and aggregation code for both scopes.
Recheck the ASDC citation page and CMR metadata before each release, the same
as for NRT.

Cite the standard collection and DOI as documented in the
[source notices](https://github.com/hypertrial/titanskies-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
