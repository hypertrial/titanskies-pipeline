# Upgrade from v0.1 to v0.2

Version 0.2 changes the derived warehouse schema and geography contract. It is
a rebuild boundary, not an in-place migration. A populated v0.1 database fails
early with a rebuild message.

1. Disable the hourly ingestion schedule.
2. Back up the v0.1 DuckDB file.
3. Build the pinned v0.2 geography artifacts.
4. Configure a new, empty DuckDB path and initialize the warehouse.
5. Run the required discovery lookback or backfill.
6. Validate administrative marts, `tempo_no2_grid_latest`, and data-quality
   output.
7. Re-enable the schedule.

Existing raw NetCDF files are reusable when they match the v0.2 native-grid
contract. Do not point v0.2 at the populated v0.1 DuckDB file or copy v0.1
derived tables into the new warehouse.
