# Third-party and source notices

Last reviewed: 2026-07-17.

The MIT licence in this repository covers Hypertrial's original software only.
It does not relicense NASA observations, boundary data, OpenStreetMap-derived
data, reference geography, or generated databases. This repository distributes
no downloaded NASA granules, signed download URLs, production boundary
archives, or generated production geography/DuckDB databases. Tracked NetCDF
and Parquet fixtures are small Hypertrial-generated synthetic files.

TitanSkies and Hypertrial are not affiliated with or endorsed by NASA, the
U.S. Census Bureau, Statistics Canada, INEGI, OpenStreetMap, or
timezone-boundary-builder.

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
- cite the NASA collection and DOI for NASA-derived measurements;
- exclude credentials, signed URLs, or material not covered by the source
  terms; and
- avoid implying provider or government endorsement.

This summary is not legal advice. Recheck source terms for the intended use and
obtain professional advice where needed.

## Dependencies

Runtime and development dependencies retain their own licences. `uv.lock` is
the authoritative version inventory. Release evidence must include a
machine-generated dependency-licence report and review of every unknown or
non-permissive result.
