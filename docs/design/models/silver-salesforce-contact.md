---
artifacts: [silver_salesforce_contact]
---

# silver.salesforce_contact

Typed, cleaned passthrough of ingested Salesforce Contact bronze data.

## Grain

One row per Salesforce Contact (unique on Contact `Id`).

## Decisions

| Decision | Reason | Requirement ref |
| --- | --- | --- |
| Dropped records without an `Id` | Records without an Id cannot be attributed | R-03@1 |
| Rename standard fields to lowercase snake_case | Project convention | R-03@1 |
| Soft-delete filter: `is_deleted = false` | Exclude soft-deleted Contacts | R-03@1 |
| Materialized as view | Project convention for staging/intermediate | — |

## Rejected

- **Deduplication by Id** — latest snapshot already ensures one-row-per-Id
- **Joining AccountId to Account** — role of gold layer; silver stays per-object

## Rerun behaviour

Identical output for identical bronze data; view reflects underlying bronze table.

## Consumers

- `marts.dim_contact`

## Supporting evidence

Salesforce Contact standard object fields; bronze pipeline design record at `docs/design/pipelines/salesforce-account-contact.md`.

## History

- 2026-09-13: Initial design (intent: `new-intent-b6b53482`)