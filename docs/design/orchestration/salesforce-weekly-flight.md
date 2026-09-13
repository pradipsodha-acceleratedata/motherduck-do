---
artifacts: [salesforce_weekly_flight]
---

# Salesforce Weekly Flight

MotherDuck Flight that orchestrates the Salesforce ingestion pipeline and dbt transformation on a weekly schedule.

## Overview

One Flight running the complete Salesforce Account+Contact pipeline: ingest from Salesforce via dlt, then run dbt build for the transformation layer.

## Pipeline inventory

| Name | Platform | Action | Invoked artifacts (ordered) | Schedule | Reason |
| --- | --- | --- | --- | --- | --- |
| `salesforce_weekly` | `motherduck` | `create` | dlt Salesforce pipeline → dbt build (transformation) | Weekly (cron: `0 6 * * 1`) | Runs ingestion then transformation every Monday at 06:00 UTC |

### Rationale

- **Single Flight** — ingestion and transformation share the same interval (weekly), owner, failure boundary, and recovery contract; there is no reason to split them
- **Weekly cadence** — user specified weekly; Monday 06:00 UTC avoids weekend contention and allows time for any manual intervention during business hours
- **Ordering** — ingestion (dlt) must complete before transformation (dbt) can run; one Flight with sequential steps guarantees this

## Design constraints

- The Flight runs on MotherDuck compute
- It connects to the `prd` database for both reading/writing bronze tables and running dbt models
- dbt runs with the project at `transformation/` using the `dbt_motherduck_prd` profile
- The dlt pipeline runs from the ingestion directory with the Salesforce connector

## Consumers

- Downstream analytics querying `prd.marts.dim_account` and `prd.marts.dim_contact`

## History

- 2026-09-13: Initial design (intent: `new-intest-b6b53482`)