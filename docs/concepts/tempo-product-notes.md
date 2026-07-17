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
