---
artifacts: [silver_salesforce_account]
---

# silver.salesforce_account

Typed, cleaned passthrough of ingested Salesforce Account bronze data.

## Grain

One row per Salesforce Account (unique on Account `Id`).

## Decisions

| Decision | Reason | Requirement ref |
| --- | --- | --- |
| Dropped records without an `Id` | Records without an Id cannot be attributed; analogous to existing stg_air_quality handling | R-03@1 |
| Rename standard fields to lowercase snake_case | Project convention: all columns in lowercase snake_case for consistency | R-03@1 |
| Soft-delete filter: `is_deleted = false` | Exclude soft-deleted records from Salesforce (standard `IsDeleted` field) | R-03@1 |
| Materialized as view | Per project convention, staging/intermediate layers are views | — |

## Rejected

- **Deduplication by Id** — the source uses `replace` disposition, so the latest snapshot is already at one-row-per-Id; no dedup needed
- **Filtering on additional criteria** — not requested; all active accounts are in scope

## Rerun behaviour

Identical output for identical bronze data; view always reflects the underlying bronze table.

## Consumers

- `marts.dim_account`

## Supporting evidence

Salesforce Account standard object fields; bronze pipeline design record at `docs/design/pipelines/salesforce-account-contact.md`.

## History

- 2026-09-13: Initial design (intent: `new-intent-b6b53482`)