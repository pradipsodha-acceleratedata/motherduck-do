---
artifacts: [dim_contact]
---

# marts.dim_contact

Gold-layer dimension table: one row per Salesforce Contact.

## Grain

One row per Contact (unique on Contact `Id`).

## Decisions

| Decision | Reason | Requirement ref |
| --- | --- | --- |
| All standard Salesforce Contact fields included | User requested all fields | R-04@1 |
| Key: `contact_id` (sourced from Contact `Id`) | Natural key from source | R-04@1 |
| Materialized as table | Project convention for marts/gold layer | — |
| Schema: `marts` | Per dbt_project.yml configuration | — |
| `account_id` preserved | Enables join to `dim_account` for downstream analysis | R-04@1 |

## Control columns

Each row carries `_loaded_at` (timestamp of bronze load) and `_dbt_invocation_id` (dbt run identifier), populated by the dbt project configuration. These enable freshness and lineage tracking downstream.

## Rejected

- **Denormalizing Account name/fields into dim_contact** — would violate dimensional modelling best practice; join via `account_id`

## Rerun behaviour

Identical output for identical silver input; fully deterministic.

## Consumers

- Downstream analytics and CRM reporting (no explicit semantic model in this intent)

## Supporting evidence

Pipeline design: `docs/design/pipelines/salesforce-account-contact.md`
Silver model design: `docs/design/models/silver-salesforce-contact.md`

## History

- 2026-09-13: Initial design (intent: `new-intent-b6b53482`)