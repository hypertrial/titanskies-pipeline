# FAQ

| Role | Start here |
| --- | --- |
| Analyst | [Analysts hub](../audiences/analysts.md) |
| Operator | [Operators hub](../audiences/operators.md) |
| Contributor | [Contributors hub](../audiences/contributors.md) |
| Integrator | [Integrators hub](../audiences/integrators.md) |

## Is TitanSkies a hosted service?

No. Operators run the software locally. Hypertrial does not host a TEMPO API.

## What is the difference between NRT and standard scopes?

`tempo:no2` uses the near-real-time product; `tempo:no2_std` uses TEMPO NO₂ L3
V04. Jobs, schemas, and contracts are independent. See
[Choose a scope](../getting-started/choose-a-scope.md).

## Are schedules enabled by default?

No. Enable only after successful manual discovery/ingest/dbt.

## Is this health or exposure advice?

No. See [Operator responsibilities](operator-responsibilities.md).

## Do I need Earthdata credentials for the demo?

No. `make demo` is credential-free and uses synthetic geography.
