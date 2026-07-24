# Query recipes

Copy-paste SQL for common analyst paths. Prefer `is_analysis_ready` for
analysis. Standard-scope mirrors use the `tempo_no2_std_*` prefix.

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

## Regional hourly series

```sql
select observation_hour, no2_mean, no2_median, no2_p90,
       coverage_fraction, source_granule_count
from tempo_no2_marts.tempo_no2_region_hourly
where canonical_region_id = 'CA-ON'
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

## Standard scope mirror

`tempo_no2_std_region_latest` already filters to analysis-ready non-country
regions and does not expose `is_analysis_ready` (same as the NRT latest mart).

```sql
select canonical_region_id, country_code, region_type,
       latest_observation_hour, latest_no2_mean,
       latest_coverage_fraction, data_age_hours
from tempo_no2_std_marts.tempo_no2_std_region_latest
order by latest_no2_mean desc
limit 25;
```

## Export CSV or Parquet

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
