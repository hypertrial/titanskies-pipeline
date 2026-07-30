# Third-party and source notices

Last reviewed: 2026-07-30.

The MIT licence in this repository covers Hypertrial's original software only.
It does not relicense NASA observations, boundary data, OpenStreetMap-derived
data, reference geography, or generated databases. This repository distributes
no downloaded NASA granules, signed download URLs, production boundary
archives, or generated production geography/DuckDB databases. Tracked NetCDF
and Parquet fixtures are small Hypertrial-generated synthetic files.

TitanSkies and Hypertrial are not affiliated with or endorsed by NASA,
PO.DAAC, the SWORD project, the U.S. Census Bureau, Statistics Canada, INEGI,
OpenStreetMap, or timezone-boundary-builder.

## Paper-reproduction source matrix

Every literal in this section mirrors the tracked manifests under
`config/reproductions/`. A preflight record is source metadata, not a licence
grant and not proof that the production object is available.

### `sun2025:repro`

#### `tempo_no2_l2_v03`

- Version: `V03`
- URL: `https://asdc.larc.nasa.gov/project/TEMPO/TEMPO_NO2_L2_V03`
- Attribution: `NASA/LARC/SD/ASDC TEMPO NO2 Level 2 V03`
- Licence field: `NASA data and information policy`

#### `facility_cohort_14`

- Version: `paper-reviewed-14`
- URL: `https://agupubs.onlinelibrary.wiley.com/doi/suppl/10.1029/2025JD044565/supinfo/2025JD044565-sup-0001-Supporting%20Information%20SI-S01.pdf`
- Attribution: `Paper authors and supplementary information`
- Licence field: `Creative Commons Attribution 4.0 International`

The tracked 14-row cohort is normalized from Supporting Information Table S1
and crosswalked to public EPA CAMD facility IDs. The Wiley article and
supplement are CC BY 4.0; the tracked CSV records the paper locator and EPA
crosswalk source and contains no emissions payload.

#### `era5_single_levels`

- Version: `ERA5`
- URL: `https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels`
- Attribution: `Copernicus Climate Change Service ERA5`
- Licence field: `Copernicus data licence`

#### `era5_pressure_levels`

- Version: `ERA5`
- URL: `https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels`
- Attribution: `Copernicus Climate Change Service ERA5`
- Licence field: `Copernicus data licence`

#### `epa_camd_hourly`

- Version: `2023-2024 reported revisions`
- URL: `https://api.epa.gov/easey/camd-services/bulk-files`
- API contract: `https://api.epa.gov/easey/camd-services/swagger/`
- Attribution: `EPA Clean Air Markets Division`
- Licence field: `United States government data`

#### `geos_cf_2024`

- Version: `paper-time 2024 archive revision unresolved`
- URL: `https://gmao.gsfc.nasa.gov/weather_prediction/GEOS-CF/`
- Attribution: `NASA GMAO GEOS Composition Forecasting`
- Licence field: `NASA data and information policy`

#### `sun2025_code`

- Version: `v0.4 / repository commit 69b397732ea011187d3e9178a1fca43e86692b94`
- URL: `https://zenodo.org/records/15001466`
- Attribution: `Kang Sun, Physical oversampling in python (POPy) v0.4`
- Licence field: `MIT License`

### `andreadis2025:repro`

#### `swot_riversp_reach_v2`

- Version: `2.0 / Version C`
- URL: `https://podaac.jpl.nasa.gov/dataset/SWOT_L2_HR_RiverSP_reach_2.0`
- Attribution: `NASA/JPL PO.DAAC SWOT RiverSP reach Version C`
- Licence field: `NASA data and information policy`

#### `swot_riversp_node_v2`

- Version: `2.0 / Version C`
- URL: `https://podaac.jpl.nasa.gov/dataset/SWOT_L2_HR_RiverSP_node_2.0`
- Attribution: `NASA/JPL PO.DAAC SWOT RiverSP node Version C`
- Licence field: `NASA data and information policy`

#### `sword_v16`

- Version: `SWORD v16`
- URL: `https://zenodo.org/records/10013982/files/SWORD_v16_gpkg.zip?download=1`
- Attribution: `SWOT River Database v16`
- Licence field: `Creative Commons Attribution 4.0 International`

#### `swot_l4_sos_paper_snapshot`

- Version: `Version 1; paper-time 2024-10-24 object generation unresolved`
- URL: `https://podaac.jpl.nasa.gov/dataset/SWOT_L4_DAWG_SOS_DISCHARGE`
- Attribution: `NASA SWOT Discharge Algorithm Working Group`
- Licence field: `NASA data and information policy`

#### `confluence_code`

- Version: `paper-time commit unresolved`
- URL: `https://github.com/SWOT-Confluence`
- Attribution: `SWOT Confluence project contributors`
- Licence field: `Repository-declared software licences`

#### `grdc_gauge_fallback`

- Version: `paper station records`
- URL: `https://portal.grdc.bafg.de/`
- Attribution: `Global Runoff Data Centre`
- Licence field: `Research-use terms; source observations are not redistributable`

GRDC observations must not be committed or redistributed. Current SWOT L4
Version 3, a later Confluence revision, or provider-reprocessed ERA5/GEOS-CF
content must retain its non-exact status through every downstream artifact.
The tracked resolution bundles contain technical metadata and evidence hashes,
not provider payloads or a licence grant. Current public evidence does not
establish the paper-time GEOS-CF v1 object revision, SWOT L4 SoS Version 1
generation, or complete Confluence repository/commit set; they remain exact
acquisition blockers.

## PlumeGraph source matrix

Every literal below mirrors `config/plumegraph_sources.json`.

### `tempo_no2_l2_v04`

- Version: `V04`
- URL: `https://asdc.larc.nasa.gov/project/TEMPO/TEMPO_NO2_L2_V04`
- CMR concept ID: `C3685896872-LARC_CLOUD`
- DOI: `10.5067/IS-40e/TEMPO/NO2_L2.004`
- Attribution: `NASA/LARC/SD/ASDC TEMPO NO2 Level 2 V04`
- Licence field: `NASA data and information policy`

The collection is provisional. PlumeGraph retains parseable AOI pixels and
derives background, plume, attribution, and emission evidence. Those
transformations are Hypertrial's, not NASA's.

### `hrrr_analysis`

- Version: `analysis-f00`
- URL: `s3://hrrrzarr`
- Attribution: `NOAA High-Resolution Rapid Refresh; public Zarr archive managed by University of Utah`
- Licence field: `United States government work; public domain`

### `epa_camd`

- Version: `2024`
- URL: `https://api.epa.gov/easey/emissions-mgmt`
- Attribution: `United States Environmental Protection Agency Power Sector Emissions Data`
- Licence field: `United States government data`

### `epa_egrid`

- Version: `eGRID2023`
- URL: `https://www.epa.gov/egrid/download-data`
- Attribution: `United States Environmental Protection Agency eGRID`
- Licence field: `United States government data`

### `epa_eia_crosswalk`

- Version: `October-2022-v0.3`
- URL: `https://www.epa.gov/power-sector/power-sector-data-crosswalk`
- Attribution: `United States Environmental Protection Agency and United States Energy Information Administration Power Sector Data Crosswalk`
- Licence field: `United States government data`

PlumeGraph uses EPA CAMD apportioned hourly emissions, facility/unit
attributes, eGRID 2023 metadata, and the EPA–EIA crosswalk. Empty values stay
missing rather than becoming zero. Preserve attribution and access dates.
Source attribution is scientific evidence, not proof or a regulatory finding.

## NASA TEMPO source

TitanSkies uses these collections:

| Field | Authoritative value |
| --- | --- |
| Short name/version | `TEMPO_NO2_L3_NRT`, `V02` |
| Native collection ID | `TEMPO_NO2_L3_NRT_V02` |
| CMR concept ID | `C3685668637-LARC_CLOUD` |
| Title | TEMPO gridded NO2 tropospheric and stratospheric columns V02 (NRT) (PROVISIONAL) |
| Creators | Caroline R Nowlan, Gonzalo González Abad, Huiqun Wang, John C Houck, and Xiong Liu |
| DOI | [10.5067/IS-40e/TEMPO/NO2_NRT_L3.002](https://doi.org/10.5067/IS-40e/TEMPO/NO2_NRT_L3.002) |
| Citation page | [NASA ASDC TEMPO_NO2_L3_NRT_V02](https://asdc.larc.nasa.gov/project/TEMPO/TEMPO_NO2_L3_NRT_V02/citation) |
| Data-use guidance | [NASA Earthdata Data Use and Citation Guidance](https://www.earthdata.nasa.gov/engage/open-data-services-software-policies/data-use-guidance) |
| Metadata access date | 2026-07-17 |

| Field | Authoritative value |
| --- | --- |
| Short name/version | `TEMPO_NO2_L3`, `V04` |
| Native collection ID | `TEMPO_NO2_L3_V04` |
| CMR concept ID | `C3685896708-LARC_CLOUD` |
| Title | TEMPO gridded NO2 tropospheric and stratospheric columns V04 (standard, PROVISIONAL) |
| Creators | Xiong Liu |
| DOI | [10.5067/IS-40E/TEMPO/NO2_L3.004](https://doi.org/10.5067/IS-40E/TEMPO/NO2_L3.004) |
| Citation page | [NASA Earthdata TEMPO_NO2_L3_V04](https://www.earthdata.nasa.gov/data/catalog/larc-cloud-tempo-no2-l3-v04) |
| Data-use guidance | [NASA Earthdata Data Use and Citation Guidance](https://www.earthdata.nasa.gov/engage/open-data-services-software-policies/data-use-guidance) |
| Metadata access date | 2026-07-18 |

The DOIs above were returned by current authoritative NASA CMR/Earthdata
metadata on their respective access dates; they were not inferred. Recheck
CMR and the citation pages before each release. NASA-led mission data without
a marked restriction are CC0 by default under Earthdata guidance, but users
should cite the dataset, describe how it was used, acknowledge NASA as the
source, respect any item-specific restriction, avoid falsely claiming
copyright in NASA material, and never imply NASA endorsement.

TitanSkies downloads granules locally, validates the operational grid and
quality flags, calculates area-weighted administrative aggregates, and retains
the latest supported-country native-grid observations. Those transformations
are Hypertrial's, not NASA's.

## NASA SWOT RiverSP and Hydrocron

RiverPulse pins the reach-only Version D collection:

| Field | Authoritative value |
| --- | --- |
| Short name/version | `SWOT_L2_HR_RiverSP_reach_D`, Version D |
| CMR concept ID | `C3233942283-POCLOUD` |
| Title | SWOT Level 2 River Single-Pass Vector Reach Data Product, Version D |
| DOI | [10.5067/SWOT-RIVERSP-D](https://doi.org/10.5067/SWOT-RIVERSP-D) |
| Collection page | [PO.DAAC RiverSP reach D](https://podaac.jpl.nasa.gov/dataset/SWOT_L2_HR_RiverSP_reach_D) |
| Timeseries service | [PO.DAAC Hydrocron](https://podaac.github.io/hydrocron/timeseries/) |
| Attribution | Surface Water Ocean Topography (SWOT). 2025. SWOT Level 2 River Single-Pass Vector Reach Data Product, Version D. PO.DAAC, CA, USA. |
| Metadata access date | 2026-07-29 |

Hydrocron returns a subset and representation of the underlying RiverSP
collection; it does not change the dataset's source identity. RiverPulse
retains raw response snapshots, normalizes reach observations and discharge
variants, and derives current-revision/readiness relations. Those
transformations are Hypertrial's, not NASA's or PO.DAAC's. Cite the Version D
dataset DOI, identify Hydrocron as the access service, state the access date,
and do not imply NASA or PO.DAAC endorsement.

## SWORD v17b network source

Every value below mirrors `config/riverpulse_sources.json`. The checksum pins
the downloaded archive bytes; it is not a licence grant.

### `sword_v17b_gpkg`

- Version: `SWORD v17b`
- URL: `https://zenodo.org/records/15299138/files/SWORD_v17b_gpkg.zip?download=1`
- Filename: `SWORD_v17b_gpkg.zip`
- Checksum algorithm: `md5`
- Checksum: `fdaaad6f6b0b58f4212b99d2ad98188c`
- Attribution: `Altenau et al., SWOT River Database (SWORD) v17b, Zenodo record 15299138`
- Licence field: `Creative Commons Attribution 4.0 International (CC BY 4.0): https://creativecommons.org/licenses/by/4.0/`
- Record: [Zenodo 15299138](https://zenodo.org/records/15299138), accessed 2026-07-29

The production RiverPulse network is a bounded, transformed subset of SWORD:
TitanSkies selects mainstem corridors, preserves boundary references, and
publishes immutable reach/edge Parquet generations. Operators redistributing a
network generation must retain the SWORD attribution and confirm the source
record's CC BY 4.0 attribution requirements for their intended use.

## Production geography source matrix

Every value in this table mirrors `config/geography_sources.json`. Checksums
pin the downloaded archive bytes; they are not licence grants.

### `us_states_2025`

- Version: `TIGER/Line 2025`
- URL: `https://www2.census.gov/geo/tiger/TIGER2025/STATE/tl_2025_us_state.zip`
- Filename: `tl_2025_us_state.zip`
- SHA-256: `59a220888a8d9be8117c4fcd38f542bd02d81abf0d198c78113595ad540dd957`
- Attribution: `U.S. Census Bureau, 2025 TIGER/Line Shapefiles`
- Licence field: `United States government work; see Census Bureau data-use terms`

### `us_counties_2025`

- Version: `TIGER/Line 2025`
- URL: `https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip`
- Filename: `tl_2025_us_county.zip`
- SHA-256: `9c6e9d9076abce2670d1de255de3710c35ecca00a7005d88e012dec52d95f763`
- Attribution: `U.S. Census Bureau, 2025 TIGER/Line Shapefiles`
- Licence field: `United States government work; see Census Bureau data-use terms`

The state and county boundaries are spatial extracts from the U.S. Census
Bureau's MAF/TIGER system. Cite the
[2025 TIGER/Line Shapefiles](https://www.census.gov/geographies/mapping-files/2025/geo/tiger-line-file.html)
and their [technical documentation](https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/tiger-geo-line.html).
TitanSkies clips, reprojects, canonicalizes, and intersects them with the TEMPO
grid. Do not imply Census Bureau endorsement.

### `canada_csd_2025`

- Version: `Census Subdivision Boundary File 2025`
- URL: `https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/lcsd000a25a_e.zip`
- Filename: `lcsd000a25a_e.zip`
- SHA-256: `80157c64de60d6a52b4239e132243bb22d6d48bd78a7f88e9710c632f940ce7f`
- Attribution: `Statistics Canada, Census Subdivision Boundary File, 2025`
- Licence field: `Statistics Canada Open Licence`
- Terms: [Statistics Canada Open Licence](https://www.statcan.gc.ca/en/terms-conditions/open-licence), accessed 2026-07-17

For an unmodified reproduction use: “Source: Statistics Canada, Census
Subdivision Boundary File, 2025. Reproduced and distributed on an ‘as is’
basis with the permission of Statistics Canada.”

TitanSkies produces an adaptation, so use: “Adapted from Statistics Canada,
Census Subdivision Boundary File, 2025. This does not constitute an endorsement
by Statistics Canada of this product.” Reproduce information accurately, do
not misrepresent it or its source, and do not use it to identify a person,
business, or organization.

### `mexico_geostatistical_2025`

- Version: `Marco Geoestadistico Integrado 2025`
- URL: `https://www.inegi.org.mx/contenidos/productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/794551163061/mg_2025_integrado.zip`
- Filename: `mg_2025_integrado.zip`
- SHA-256: `f1335bab72d5582adab06e9e3b5d49b7c42da8f8d82e588f484ad6a7c7871d1b`
- Attribution: `INEGI, Marco Geoestadistico, 2025`
- Licence field: `INEGI terms of free use`
- Terms: [INEGI Términos de uso](https://www.inegi.org.mx/inegi/terminos.html), accessed 2026-07-17

Credit `Fuente: INEGI, Marco Geoestadístico Integrado 2025` and include the
update date where applicable. TitanSkies reprojects, canonicalizes, clips, and
intersects the information with the TEMPO grid. Tell downstream users about
those transformations, do not present them as performed by INEGI, and do not
imply an official INEGI position, approval, sponsorship, or endorsement.

### `land_timezones_2026b`

- Version: `timezone-boundary-builder 2026b comprehensive land timezones`
- URL: `https://github.com/evansiroky/timezone-boundary-builder/releases/download/2026b/timezones.geojson.zip`
- Filename: `timezones-2026b.geojson.zip`
- SHA-256: `f892b57ce8c7d9633a03ce9e6775d54544c05d9b8d62029bc6543091cac213c4`
- Attribution: `timezone-boundary-builder contributors and OpenStreetMap contributors`
- Licence field: `Open Data Commons Open Database License (ODbL) 1.0`
- Terms: [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/), accessed 2026-07-17

The timezone boundaries derive from timezone-boundary-builder and
OpenStreetMap data. Attribute both contributor communities and link ODbL 1.0.
Public use of a Produced Work requires an ODbL source notice. Publicly used
Derivative Databases are subject to ODbL share-alike, notice, and
machine-readable access obligations. TitanSkies assigns dominant IANA
timezones to regions; operators redistributing a generated geography database
must determine and satisfy the applicable ODbL obligations.

## Generated geography and warehouses

Operators control local artifacts but do not acquire ownership of source
rights. Anyone redistributing generated registries, overlap weights, DuckDB
warehouses, exports, maps, or analyses must:

- preserve source-specific attribution and transformation notices;
- comply with the Statistics Canada and INEGI terms;
- satisfy ODbL notice/share-alike/access duties where applicable;
- cite the applicable NASA TEMPO or SWOT collection and DOI for NASA-derived
  measurements;
- preserve SWORD attribution for RiverPulse network derivatives;
- exclude credentials, signed URLs, or material not covered by the source
  terms; and
- avoid implying provider or government endorsement.

This summary is not legal advice. Recheck source terms for the intended use and
obtain professional advice where needed.

## Documentation fonts

Documentation fonts under `docs/assets/fonts/` are distributed under their
included SIL Open Font License notices.

## Dependencies

Runtime and development dependencies retain their own licences. `uv.lock` is
the authoritative version inventory. Release evidence must include a
machine-generated dependency-licence report and review of every unknown or
non-permissive result.
