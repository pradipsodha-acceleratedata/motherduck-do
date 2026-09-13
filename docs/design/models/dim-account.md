---
artifacts: [dim_account]
---

# marts.dim_account

Gold-layer dimension table: one row per Salesforce Account.

## Grain

One row per Account (unique on Account `Id`).

## Decisions

| Decision | Reason | Requirement ref |
| --- | --- | --- |
| All standard Salesforce Account fields included | User requested all fields | R-04@1 |
| Key: `account_id` (sourced from Account `Id`) | Natural key from source | R-04@1 |
| Materialized as table | Project convention for marts/gold layer | — |
| Schema: `marts` | Per dbt_project.yml configuration | — |

## Rejected

- **Joining Contact data** — Contact is a separate dimension; any relationship is implicit via `account_id` on Contact

## Rerun behaviour

Identical output for identical silver input; fully deterministic.

## Consumers

- Downstream analytics and CRM reporting (no explicit semantic model in this intent)

## Supporting evidence

Pipeline design: `docs/design/pipelines/salesforce-account-contact.md`
Silver model design: `docs/design/models/silver-salesforce-account.md`

## History

- 2026-09-13: Initial design (intent: `new-intent-b6b53482`)