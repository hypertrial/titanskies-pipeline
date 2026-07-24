# Naming

Source: `tempo`

Scopes: `no2` (NRT) and `no2_std` (standard V04)

NRT schemas (`tempo_no2_*`):

- `tempo_no2_raw`
- `tempo_no2_ops`
- `tempo_no2_staging`
- `tempo_no2_intermediate`
- `tempo_no2_marts`
- `tempo_no2_observability`

Standard schemas (`tempo_no2_std_*`):

- `tempo_no2_std_raw`
- `tempo_no2_std_ops`
- `tempo_no2_std_staging`
- `tempo_no2_std_intermediate`
- `tempo_no2_std_marts`
- `tempo_no2_std_observability`

Asset keys follow `tempo/no2/<layer>/<entity>` and
`tempo/no2_std/<layer>/<entity>`. The NRT geography registry relation remains
`tempo_region_registry`; the standard mirror is `tempo_no2_std_region_registry`.
