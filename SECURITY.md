# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.3.x   | Yes       |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report security issues through one of these channels:

1. **GitHub Private Vulnerability Reporting** (preferred): open the repository on GitHub, go to **Security** → **Report a vulnerability**, and submit a private report.
2. **Maintainer contact**: if private reporting is unavailable, contact the repository maintainers through Hypertrial's standard security contact process.

Include:

- A description of the issue and potential impact
- Steps to reproduce (proof of concept if available)
- Affected versions or commits
- Suggested fix or mitigation, if you have one

We will acknowledge receipt and work with you on a timeline for investigation and disclosure.

## Scope notes

TitanSkies is a **local-first** NASA TEMPO NO₂ data pipeline. The current
implementation reads public NASA metadata and stores downloaded observations
and derived regional analytics in a local DuckDB warehouse. NASA Earthdata
credentials are user-supplied and must never be committed to the repository.
Never include signed URLs, live NetCDF content, downloaded boundaries, or
warehouse contents in a report. TitanSkies has no telemetry.
