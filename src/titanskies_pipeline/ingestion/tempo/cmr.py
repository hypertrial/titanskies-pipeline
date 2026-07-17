"""NASA CMR granule discovery via earthaccess."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Sequence

from titanskies_pipeline.config.settings import TEMPO_NO2_CMR_CONCEPT_ID

SearchGranulesFn = Callable[..., Sequence[Any]]


@dataclass(frozen=True)
class DiscoveredGranule:
    granule_id: str
    concept_id: str
    acquisition_start: datetime | None
    acquisition_end: datetime | None
    download_url: str | None
    cmr_revision_at: datetime | None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        return None


def _granule_id(granule: Any) -> str:
    if hasattr(granule, "get"):
        meta = granule.get("meta") or {}
        if isinstance(meta, dict):
            for key in ("native-id", "concept-id", "GranuleUR", "granule_ur"):
                if meta.get(key):
                    return str(meta[key])
        umm = granule.get("umm") or {}
        if isinstance(umm, dict) and umm.get("GranuleUR"):
            return str(umm["GranuleUR"])
        for key in ("GranuleUR", "granule_ur", "id"):
            if granule.get(key):
                return str(granule[key])
    for attr in ("granule_ur", "GranuleUR", "concept-id"):
        if hasattr(granule, attr):
            value = getattr(granule, attr)
            if value:
                return str(value)
    raise ValueError("Granule record missing identifier")


def _granule_urls(granule: Any) -> list[str]:
    data_links = getattr(granule, "data_links", None)
    if callable(data_links):
        urls = data_links()
    elif data_links:
        urls = data_links
    elif isinstance(granule, dict):
        urls = granule.get("links") or granule.get("data_links") or []
    else:
        urls = []
    parsed = []
    for item in urls:
        value = item.get("href") if isinstance(item, dict) else item
        if value and str(value).strip():
            parsed.append(str(value).strip())
    return parsed


def _mapping(granule: Any, key: str) -> dict[str, Any]:
    if hasattr(granule, "get"):
        value = granule.get(key)
        return value if isinstance(value, dict) else {}
    return {}


def _value(granule: Any, key: str) -> Any:
    if hasattr(granule, "get"):
        value = granule.get(key)
        if value is not None:
            return value
    return getattr(granule, key, None)


def _acquisition_range(granule: Any) -> tuple[datetime | None, datetime | None]:
    extent = _mapping(granule, "umm").get("TemporalExtent") or {}
    range_time = extent.get("RangeDateTime") or {}
    single = extent.get("SingleDateTime")
    if isinstance(single, dict):
        single = single.get("Time")
    start = _parse_datetime(range_time.get("BeginningDateTime") or single)
    end = _parse_datetime(range_time.get("EndingDateTime") or single)
    if start is None:
        start = _parse_datetime(_value(granule, "time_start"))
    if end is None:
        end = _parse_datetime(_value(granule, "time_end"))
    return start, end


def _cmr_revision_at(granule: Any) -> datetime | None:
    meta = _mapping(granule, "meta")
    return _parse_datetime(
        meta.get("revision-date")
        or meta.get("revision_date")
        or _value(granule, "updated")
    )


def _preferred_netcdf_url(urls: list[str]) -> str | None:
    usable = [url for url in urls if url]
    for url in usable:
        path = url.split("?", 1)[0].casefold()
        if path.endswith((".nc", ".nc4", ".netcdf")):
            return url
    return usable[0] if usable else None


def discover_granules(
    *,
    lookback_hours: int,
    concept_id: str = TEMPO_NO2_CMR_CONCEPT_ID,
    now: datetime | None = None,
    search_fn: SearchGranulesFn | None = None,
) -> list[DiscoveredGranule]:
    if lookback_hours < 1:
        raise ValueError("lookback_hours must be >= 1")
    current = now or datetime.now(timezone.utc)
    end = (
        current
        if current.tzinfo is None
        else current.astimezone(timezone.utc).replace(tzinfo=None)
    )
    start = end - timedelta(hours=lookback_hours)
    temporal = (
        start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    if search_fn is None:
        import earthaccess

        search_fn = earthaccess.search_data

    results = search_fn(concept_id=concept_id, temporal=temporal)
    discovered: list[DiscoveredGranule] = []
    for granule in results:
        granule_id = _granule_id(granule)
        urls = _granule_urls(granule)
        acquisition_start, acquisition_end = _acquisition_range(granule)
        discovered.append(
            DiscoveredGranule(
                granule_id=granule_id,
                concept_id=concept_id,
                acquisition_start=acquisition_start,
                acquisition_end=acquisition_end,
                download_url=_preferred_netcdf_url(urls),
                cmr_revision_at=_cmr_revision_at(granule),
            )
        )
    return discovered


__all__ = ["DiscoveredGranule", "discover_granules"]
