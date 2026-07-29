# RiverPulse product notes

RiverPulse v0.5 pins the PO.DAAC collection
`SWOT_L2_HR_RiverSP_reach_D` and SWORD v17b. The lane covers bounded
mainstem corridors for the Sacramento River, Rhine, and Murray River.
Hydrocron supplies reach-scale observations; SWORD exclusively supplies
geometry and topology.

Collection begins at `2023-08-01T00:00:00Z`. Backfills use calendar-year,
half-open windows; routine discovery covers a rolling 90 days. Requests are
serial, at least one second apart, and bounded by Hydrocron's 6 MB response
limit. Raw response bodies are retained under the operator's configured
RiverPulse raw root.

Every parseable row is retained. Stable observation identity combines
collection family, reach, observation time, cycle, and pass. A revision adds
CRID, granule identity, and canonical record content. Current selection ranks
source `ingest_time`, local snapshot collection time, then revision ID, so
rediscovering an older response cannot replace a newer current revision.

The tracked `riverpulse_events_contract.csv` is the sole science policy.
Readiness requires matching collection/network versions, official good
quality, and finite measurement and uncertainty values. WSE, width, slope,
and discharge have independent readiness flags; overall analysis readiness
requires WSE, width, and slope readiness. Quality never causes source-row
filtering.

This milestone excludes gauges, event detection, nodes, direct RiverSP
archives, object storage, release exports, SDK/API, and explorer work.
