# Query the warehouse

Connect DuckDB to the path printed by `make demo`, or to `DUCKDB_PATH` for a
configured warehouse. Query public relations in `tempo_no2_marts`; use
`tempo_no2_observability` to decide whether data is ready to trust.

## Latest analysis-ready regions

```sql
select canonical_region_id, country_code, region_type,
       latest_observation_hour, latest_no2_mean,
       latest_coverage_fraction, data_age_hours
from tempo_no2_marts.tempo_no2_region_latest
order by latest_no2_mean desc;
```

## Country history

```sql
select observation_hour, no2_mean, no2_p90, coverage_fraction
from tempo_no2_marts.tempo_no2_country_hourly
where country_code = 'CA'
  and is_analysis_ready
order by observation_hour;
```

## Regional anomalies

```sql
select canonical_region_id, local_observation_hour, no2_difference,
       robust_z_score, baseline_sample_count
from tempo_no2_marts.tempo_no2_region_anomalies
where canonical_region_id like 'US-%'
  and is_analysis_ready
order by abs(robust_z_score) desc nulls last;
```

## Native-grid bounding box

The grid mart publishes only the latest supported-country observation for each
native 0.02° cell. It does not resample or store WKT. Bounds are
`longitude ± 0.01°` and `latitude ± 0.01°`.

```sql
select observation_hour, latitude, longitude, no2
from tempo_no2_marts.tempo_no2_grid_latest
where is_analysis_ready
  and latitude between 43.5 and 44.0
  and longitude between -80.0 and -79.5;
```

## Freshness and quality

```sql
select granule_id, processing_status, observation_start, processed_at
from tempo_no2_observability.tempo_no2_granule_observability
order by observation_start desc
limit 25;

select canonical_region_id, observation_hour, issue_type, severity
from tempo_no2_observability.tempo_no2_data_quality
order by observation_hour desc, severity desc;
```

## Export CSV or Parquet

DuckDB exports filtered results without a separate utility:

```sql
copy (
  select *
  from tempo_no2_marts.tempo_no2_region_hourly
  where country_code = 'CA' and is_analysis_ready
) to 'canada_no2.csv' (header, delimiter ',');

copy (
  select *
  from tempo_no2_marts.tempo_no2_grid_latest
  where latitude between 43.5 and 44.0
    and longitude between -80.0 and -79.5
) to 'toronto_grid.parquet' (format parquet, compression zstd);
```

Public marts are `tempo_no2_region_hourly`, `tempo_no2_region_latest`,
`tempo_no2_region_anomalies`, `tempo_no2_country_hourly`,
`tempo_no2_grid_latest`, and `tempo_region_registry`. Their grains and fields
are defined in the [Data dictionary](../reference/data-dictionary.md).
