"""Static registry of shipped source/scope orchestration surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from titanskies_pipeline.naming import SCOPE_NO2, SOURCE_TEMPO, flat_name

ScopeStep = Literal["discovery", "ingest", "dbt", "full"]
SCOPE_STEPS: tuple[ScopeStep, ...] = ("discovery", "ingest", "dbt", "full")


@dataclass(frozen=True)
class ScopeSpec:
    source: str
    scope: str
    label: str
    discovery_job_name: str
    ingest_job_name: str
    dbt_job_name: str
    full_job_name: str
    dbt_select: str
    dbt_exclude: str | None = None

    @property
    def key(self) -> str:
        return f"{self.source}:{self.scope}"

    @property
    def namespace(self) -> str:
        return flat_name(self.source, self.scope)

    @property
    def aliases(self) -> tuple[str, str]:
        return (self.key, self.namespace)

    @property
    def supported_steps(self) -> tuple[ScopeStep, ...]:
        return SCOPE_STEPS

    def job_for_step(self, step: ScopeStep) -> str:
        return {
            "discovery": self.discovery_job_name,
            "ingest": self.ingest_job_name,
            "dbt": self.dbt_job_name,
            "full": self.full_job_name,
        }[step]


TEMPO_NO2_SCOPE = ScopeSpec(
    source=SOURCE_TEMPO,
    scope=SCOPE_NO2,
    label="TEMPO NO2",
    discovery_job_name="tempo_no2_granule_discovery",
    ingest_job_name="tempo_no2_hourly_ingest",
    dbt_job_name="tempo_no2_dbt_build",
    full_job_name="tempo_no2_full_pipeline",
    dbt_select="+tag:tempo,tag:no2",
)

SHIPPED_SCOPE_SPECS: tuple[ScopeSpec, ...] = (TEMPO_NO2_SCOPE,)


def iter_scope_specs(*, source: str | None = None) -> tuple[ScopeSpec, ...]:
    if source is None:
        return SHIPPED_SCOPE_SPECS
    return tuple(spec for spec in SHIPPED_SCOPE_SPECS if spec.source == source)


def get_scope_spec(ref: str) -> ScopeSpec:
    ref = ref.strip()
    for spec in SHIPPED_SCOPE_SPECS:
        if ref in spec.aliases:
            return spec
    known = ", ".join(spec.key for spec in SHIPPED_SCOPE_SPECS)
    raise ValueError(f"Unknown scope {ref!r}; expected one of: {known}")


__all__ = [
    "SCOPE_STEPS",
    "SHIPPED_SCOPE_SPECS",
    "ScopeSpec",
    "ScopeStep",
    "TEMPO_NO2_SCOPE",
    "get_scope_spec",
    "iter_scope_specs",
]
