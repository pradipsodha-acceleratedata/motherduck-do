---
artifacts: [salesforce_account_contact]
---

# Salesforce Account & Contact ingestion

## Grain

One row per Salesforce record (one per Account ID, one per Contact ID). Landed as two bronze tables: `bronze.salesforce_account` and `bronze.salesforce_contact`.

## Decisions

| Decision | Reason | Requirement ref |
| --- | --- | --- |
| Latest snapshot (replace) | User requested latest snapshot only; no SCD/history needed | R-02@1 |
| All standard fields ingested | User specified all fields for both Account and Contact | R-02@1 |
| Two separate bronze tables (one per object) | Each object has distinct schema; keeping them separate preserves column-level contract enforcement | R-02@1 |
| Write disposition: `replace` | Latest snapshot — each run replaces prior data, reflecting the current Salesforce state | R-02@1 |
| Schema contract: columns `freeze`, tables `evolve`, data_type `freeze` | Default per pipeline inventory template; columns frozen after first load pins the known schema | R-02@1 |
| Source not yet configured | Connector must be added via `add-or-update-source` before discovery or ingestion | R-01@1 |

## Rejected

- **Full history / merge** — not requested; adds complexity with no current consumer need
- **Single combined table** — would couple unrelated schemas and break per-object contract enforcement

## Rerun behaviour

Each run replaces all rows (`replace` disposition). Output is deterministic for a given Salesforce snapshot.

## Consumers

- `silver.salesforce_account` dbt model
- `silver.salesforce_contact` dbt model
- `marts.dim_account` dbt model
- `marts.dim_contact` dbt model

## Supporting evidence

Salesforce standard object schemas (Account and Contact) — known via dlt `salesforce` verified source connector documentation.

## Gotchas

- Salesforce connector requires OAuth credentials — will need to configure via `add-or-update-source`
- dlt `salesforce` source exports Account as `account` and Contact as `contact` resource names

## History

- 2026-09-13: Initial design (intent: `new-intent-b6b53482`)