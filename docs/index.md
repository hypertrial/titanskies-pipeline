---
hide:
  - navigation
  - toc
---

<div class="ts-hero" markdown>

<div class="ts-hero__copy" markdown>

<span class="ts-eyebrow">Local-first NASA Earth observation warehouse</span>

# TitanSkies Pipeline

Build inspectable NASA TEMPO NO₂ and SWOT river warehouses with Dagster,
DuckDB, and dbt.

Hypertrial-owned MIT software. No hosted service or health advice.
[Licence scope](concepts/scope-and-non-goals.md) ·
[Operator responsibilities](concepts/operator-responsibilities.md).

[Get started](getting-started/index.md){ .md-button .md-button--primary }
[Query the warehouse](guides/query-the-warehouse.md){ .md-button }

</div>

<div class="ts-hero__mark">
  <span>TitanSkies</span>
  <span>Pipeline</span>
</div>

</div>

<div class="ts-install" markdown>

**Start in the repository**

```bash
uv sync --locked --extra dev
```

</div>

## Start with a task

<div class="ts-task-grid" markdown>

<article class="ts-task-card" markdown>

### Analyze the data

Open a local DuckDB warehouse, filter on `is_analysis_ready`, and use tested
SQL recipes.

[Analysts hub](audiences/analysts.md)

</article>

<article class="ts-task-card" markdown>

### Operate the pipeline

Build the demo or live path, keep schedules disabled, then keep the warehouse
healthy.

[Operators hub](audiences/operators.md)

</article>

<article class="ts-task-card" markdown>

### Contribute code

Change ingestion, geography, dbt marts, or docs with the right quality gate.

[Contributors hub](audiences/contributors.md)

</article>

<article class="ts-task-card" markdown>

### Integrate downstream

Consume public marts without treating pipeline output as health advice.

[Integrators hub](audiences/integrators.md)

</article>

</div>

## Supported local scopes

Version `0.5.x` ships `tempo:no2` (near-real-time), `tempo:no2_std` (standard
V04), and `riverpulse:events` (SWOT RiverSP Version D). This site is software
documentation and does not host datasets.

[Choose a scope](getting-started/choose-a-scope.md), read the
[FAQ](concepts/faq.md), or review the
[architecture](concepts/architecture.md) before extending the pipeline.
