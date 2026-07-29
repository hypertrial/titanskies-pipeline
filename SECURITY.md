# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.7.x   | Yes       |
| 0.6.x   | No        |
| 0.5.x   | No        |
| 0.4.x   | No        |
| 0.3.x   | No        |

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

TitanSkies is a **local-first** NASA TEMPO NO₂ and SWOT river data pipeline.
The implementation reads public NASA metadata and stores downloaded
observations and derived analytics in a local DuckDB warehouse. NASA Earthdata
credentials and optional Hydrocron API keys are user-supplied and must never
be committed to the repository. Never include credentials, signed URLs, live
NetCDF or Hydrocron response content, downloaded boundary/SWORD archives,
network generations, or warehouse contents in a report. TitanSkies has no
telemetry.
