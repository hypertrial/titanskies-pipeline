# Query recipes

Copy-paste SQL for common analyst paths. Prefer `is_analysis_ready` on hourly,
country, anomaly, and grid marts. `*_region_latest` already filters to
analysis-ready non-country regions and does **not** expose that column.
Standard-scope mirrors use the `tempo_no2_std_*` prefix; they may be absent in
the NRT-only demo warehouse.

Open the demo with `uv run make demo`, then query `.cache/demo.duckdb`.

## Latest analysis-ready regions

```sql
select canonical_region_id, country_code, region_type,
       latest_observation_hour, latest_no2_mean,
       latest_coverage_fraction, data_age_hours
from tempo_no2_marts.tempo_no2_region_latest
order by latest_no2_mean desc;
```

## Regional hourly series

```sql
select observation_hour, no2_mean, no2_median, no2_p90,
       coverage_fraction, source_granule_count, is_analysis_ready
from tempo_no2_marts.tempo_no2_region_hourly
where canonical_region_id = 'US-CA-037'
  and is_analysis_ready
order by observation_hour;
```

## Country hourly history

```sql
select observation_hour, no2_mean, no2_p90, coverage_fraction,
       analysis_ready_region_count, region_count
from tempo_no2_marts.tempo_no2_country_hourly
where country_code = 'CA'
  and is_analysis_ready
order by observation_hour;
```

## Regional anomalies

```sql
select canonical_region_id, local_observation_hour, no2_mean,
       no2_difference, robust_z_score, baseline_sample_count
from tempo_no2_marts.tempo_no2_region_anomalies
where canonical_region_id like 'US-%'
  and is_analysis_ready
order by abs(robust_z_score) desc nulls last
limit 50;
```

## Native-grid bounding box

```sql
select observation_hour, latitude, longitude, no2, granule_id
from tempo_no2_marts.tempo_no2_grid_latest
where is_analysis_ready
  and latitude between 33.5 and 34.5
  and longitude between -118.5 and -117.5;
```

## Data quality and stale rows

```sql
select canonical_region_id, observation_hour, issue_type, severity,
       coverage_fraction, message
from tempo_no2_observability.tempo_no2_data_quality
where issue_type in ('stale', 'low_coverage', 'zero_valid')
order by observation_hour desc, severity desc
limit 50;
```

## Granule observability

```sql
select granule_id, processing_status, acquisition_start, processed_at,
       processing_latency_minutes, error_message
from tempo_no2_observability.tempo_no2_granule_observability
order by acquisition_start desc
limit 25;
```

## Geography registry (NRT FQN)

```sql
select canonical_region_id, region_name, region_type, country_code,
       timezone, geometry_version, geometry_checksum
from tempo_no2_marts.tempo_region_registry
where country_code = 'CA'
order by region_type, canonical_region_id
limit 50;
```

## Coverage below contract floor (hourly)

```sql
select canonical_region_id, observation_hour, coverage_fraction,
       valid_area_km2, total_area_km2, is_analysis_ready
from tempo_no2_marts.tempo_no2_region_hourly
where not is_analysis_ready
order by observation_hour desc, coverage_fraction
limit 50;
```

## Join latest NO₂ with registry labels

```sql
select latest.canonical_region_id, registry.region_name,
       latest.latest_observation_hour, latest.latest_no2_mean,
       latest.latest_coverage_fraction, latest.data_age_hours
from tempo_no2_marts.tempo_no2_region_latest as latest
inner join tempo_no2_marts.tempo_region_registry as registry
  on latest.canonical_region_id = registry.canonical_region_id
order by latest.latest_no2_mean desc
limit 25;
```

## Standard scope mirror (no `is_analysis_ready` on latest)

`tempo_no2_std_region_latest` already filters to analysis-ready non-country
regions and does not expose `is_analysis_ready` (same as the NRT latest mart).
Skipped by recipe-smoke when the demo warehouse has no standard marts.

```sql
select canonical_region_id, country_code, region_type,
       latest_observation_hour, latest_no2_mean,
       latest_coverage_fraction, data_age_hours
from tempo_no2_std_marts.tempo_no2_std_region_latest
order by latest_no2_mean desc
limit 25;
```

## Standard registry FQN

```sql
select canonical_region_id, region_name, region_type,
       geometry_version, geometry_checksum
from tempo_no2_std_marts.tempo_no2_std_region_registry
order by canonical_region_id
limit 25;
```

## Export CSV

```sql
copy (
  select *
  from tempo_no2_marts.tempo_no2_region_hourly
  where country_code = 'CA' and is_analysis_ready
) to 'canada_no2.csv' (header, delimiter ',');
```

## Export Parquet

```sql
copy (
  select *
  from tempo_no2_marts.tempo_no2_grid_latest
  where is_analysis_ready
    and latitude between 33.5 and 34.5
    and longitude between -118.5 and -117.5
) to 'la_grid.parquet' (format parquet, compression zstd);
```
