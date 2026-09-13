---
kinds: [ingestion, transformation, orchestration]
---

# Intent: Salesforce Account and Contact

## Classification

- **Action**: work — building new ingestion pipelines, transformation models, and orchestration for Salesforce data
- **Objective**: Ingest Salesforce Account and Contact objects, model them as gold-layer dimensions, orchestrate weekly, and ship as an intent PR
- **Destination**: `prd` MotherDuck database (same as existing `motherduck-do` project)
- **Rationale**: Each kind contributes a distinct artifact family:
  - `ingestion` — dlt pipeline to land Account and Contact as bronze tables
  - `transformation` — dbt models to produce silver intermediate and gold dimension marts
  - `orchestration` — MotherDuck Flight to run the pipeline weekly

## Goal

Add Salesforce CRM data (Account and Contact objects) to the existing `motherduck-do` data platform, providing gold-layer dimension tables for downstream analytics and CRM reporting. The data is consumer-facing: analysts querying `prd` for account and contact dimensions.

## Source system

Salesforce, via dlt's `salesforce` verified source. The connector is **not yet configured** — it must be added (`add-or-update-source`) as the first ingestion task. All standard fields for both the Account and Contact objects are in scope.

## Target

- **Platform**: MotherDuck
- **Database**: `prd` (existing)
- **Schemas**: bronze (landed raw), silver (deduplicated/cleaned), marts (gold dimensions)
- **Convention**: following the existing project pattern — staging/silver as views, marts/gold as tables

## Deliverables inventory

| # | Deliverable | Kind | Requirement refs | Notes |
| --- | --- | --- | --- | --- |
| 1 | Salesforce connector configuration | ingestion | R-01@1 | `add-or-update-source`, connector not yet configured |
| 2 | Account ingestion pipeline (dlt) | ingestion | R-02@1 | All standard fields, latest snapshot (replace) |
| 3 | Contact ingestion pipeline (dlt) | ingestion | R-02@1 | All standard fields, latest snapshot (replace) |
| 4 | Silver Account model (dbt) | transformation | R-03@1 | Cleaned, deduplicated intermediate |
| 5 | Silver Contact model (dbt) | transformation | R-03@1 | Cleaned, deduplicated intermediate |
| 6 | `dim_account` gold mart (dbt) | transformation | R-04@1 | One row per Account |
| 7 | `dim_contact` gold mart (dbt) | transformation | R-04@1 | One row per Contact |
| 8 | MotherDuck Flight (weekly orchestration) | orchestration | R-05@1 | Runs dlt pipelines then dbt build |
| 9 | dbt source registration for bronze tables | ingestion (Ship) | R-06@1 | Register landed bronze in `_sources.yml` |
| 10 | CI/CD pipeline | — | R-07@1 | Standard CI bundle (see Out of scope for availability) |

## Requirements

| ID | Revision | Requirement | Acceptance criteria | Source | Resolution | Status |
| --- | --- | --- | --- | --- | --- | --- |
| R-01 | 1 | Configure Salesforce as a named data source connection in the workspace | Source connection exists and passes connection test | User request: "connector not configured" | supplied decision | pending |
| R-02 | 1 | Ingest Salesforce Account and Contact objects as bronze tables in the `prd` database | Bronze tables exist with correct schema, all standard Salesforce fields, latest snapshot (replace write disposition) | User request: "ingest account and contact from salesforce", "latest snapshot", "all fields" | supplied decision | pending |
| R-03 | 1 | Create silver-layer dbt models for Account and Contact with cleaned, typed, and deduplicated data | Silver models reference bronze sources, produce deterministic output, pass dbt data tests | Derived from medallion architecture convention | derived fact | pending |
| R-04 | 1 | Create gold-layer `dim_account` and `dim_contact` marts as tables in the `prd` database, one row per entity | Gold models exist as tables, acceptably populated, pass dbt data tests (unique + not-null on keys) | User request: "gold tables", "dim_account + dim_contact" | user decision | pending |
| R-05 | 1 | Author a MotherDuck Flight that runs the ingestion pipeline then the dbt transformation on a weekly schedule | Flight is committed at `orchestration/<Name>.Flight/`, executes end-to-end, runs weekly | User request: "orchestration", "weekly" | user decision | pending |
| R-06 | 1 | Register landed bronze tables as dbt sources in the dbt project | `_sources.yml` contains entries for the ingested bronze tables with correct database/schema/table | Derived from ingestion Ship stage | derived fact | pending |
| R-07 | 1 | Install standard CI bundle for the domain repository | `.workflow/ci-config.yml` exists and passes validation | User request: "add cicd" | supplied decision | deferred |

## Out of scope

- **Salesforce Opportunities, Leads, or other objects** — only Account and Contact are in scope
- **Historical loading / SCD** — latest snapshot only (replace write disposition)
- **Fact tables or metrics** — gold layer is dimension-only (`dim_account`, `dim_contact`)
- **Custom Salesforce fields** — standard fields only
- **CI/CD** — `VD_CI_BUNDLE_DIR` is not set for this MotherDuck domain; standard CI bundle installation is unavailable. The requirement is recorded as deferred pending domain infrastructure setup.
- **Semantic models / MetricFlow** — not requested; no semantic model artifacts needed

## Open questions

- None — all business decisions resolved above.

## Design pending

- Salesforce connector details (authentication method, instance URL) — resolved during `add-or-update-source`
- Exact bronze schema shape — discovered via `discovering-source-schema` after source is configured
- Silver model grain and dedup strategy — resolved via `applying-medallion-data-modelling`
- Flight name and exact invoked artifact ordering — resolved during orchestration design

## Change history

*Initial version.* 2026-09-13.

## Approvals

*Pending — to be recorded after user review.*