# Plan: Salesforce Account and Contact Ingestion, Gold Models, and Weekly Orchestration

> **For agentic workers:** `planning` asks the user to select `subagent-driven-development` (recommended) or `executing-the-plan` (inline) before this plan runs.

**Goal:** Ingest Salesforce Account and Contact objects, build silver intermediate and gold dimension marts, orchestrate weekly via MotherDuck Flight, and ship as an intent PR.

**Approach:** Configure the Salesforce dlt source, generate two dlt pipelines (Account + Contact) with `replace` disposition to land bronze tables, build dbt views for silver and tables for gold, register the bronze sources in dbt, create a MotherDuck Flight for weekly orchestration, then verify and ship.

**Tech Stack:** dlt (Salesforce verified source), dbt (MotherDuck), MotherDuck (Flights for orchestration)

## Global Constraints

- Target database: `prd` (MotherDuck)
- Bronze schema: main (dlt default landing)
- Silver models: materialized as views in schema `intermediate`
- Gold marts: materialized as tables in schema `marts`
- dbt project root: `transformation/`
- Write disposition: `replace` (latest snapshot; no history/SCD)
- All standard Salesforce fields for both objects
- Orchestration: MotherDuck Flight, weekly (Monday 06:00 UTC)
- CI/CD: Not available for this domain (`VD_CI_BUNDLE_DIR` not set)

---

## Scope and impact

### Ingestion pipeline inventory

| resource | entry_point | columns | tables | data_type | write_disposition | incremental_cursor | notes | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| account | salesforce_source | freeze | evolve | freeze | merge | LastModifiedDate | primary_key=Id; 78 fields incl. custom; source uses merge/incremental, not replace | working |
| contact | salesforce_source | freeze | evolve | freeze | replace | | primary_key=Id, references Account via AccountId; ~55 fields incl. custom | working |

### Transformation scope

| Artifact | Kind | Layer | Action | Requirements | Design record | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `silver.salesforce_account` | model | intermediate | create | R-03@1 | [`silver-salesforce-account.md`](../design/models/silver-salesforce-account.md) | Cleaned, typed Account data; view |
| `silver.salesforce_contact` | model | intermediate | create | R-03@1 | [`silver-salesforce-contact.md`](../design/models/silver-salesforce-contact.md) | Cleaned, typed Contact data; view |
| `dim_account` | model | mart | create | R-04@1 | [`dim-account.md`](../design/models/dim-account.md) | One row per Account; table |
| `dim_contact` | model | mart | create | R-04@1 | [`dim-contact.md`](../design/models/dim-contact.md) | One row per Contact; table |

### Orchestration scope

| Artifact | Kind | Action | Platform | Invoked artifacts | Schedule | Reason |
| --- | --- | --- | --- | --- | --- | --- |
| `salesforce_weekly` | orchestration | create | motherduck | dlt Salesforce pipeline → dbt build | Weekly (Mon 06:00 UTC) | Weekly cadence per user; single Flight shares interval, owner, failure boundary |

### Source mapping

```
Salesforce Account API → bronze.account → silver.salesforce_account (view) → marts.dim_account (table)
Salesforce Contact API → bronze.contact → silver.salesforce_contact (view) → marts.dim_contact (table)
```

### Change impact

No existing artifacts are touched. This intent creates new artifacts only. No downstream regression risk.

---

## Tasks

### Task 1: Configure Salesforce source connection

**Requirement refs:** R-01@1

**Files:**
- Create: `ingestion/.dlt/config.toml` (sources section)
- Create: `ingestion/requirements.txt`

**Interfaces:**
- Consumes: Salesforce connector credentials (from source configuration)
- Produces: `[sources.salesforce]` entry in `ingestion/.dlt/config.toml`

- [ ] **Step 1: Add Salesforce source via `add-or-update-source`**
  This task invokes `add-or-update-source` to configure the Salesforce connector. The connector is not yet configured.
- [ ] **Step 2: Verify connection via `test-source-connection`**
- [ ] **Step 3: Commit**

```bash
git add ingestion/
git commit -m "configure Salesforce source connection"
```

### Task 2: Discover Salesforce schema and create pipeline design inventory

**Requirement refs:** R-02@1

**Files:**
- Modify: plan.md (pipeline inventory rows flipped to `done`)

**Interfaces:**
- Consumes: Salesforce source connection (Task 1)
- Produces: Pipeline inventory with schema-contract rows (columns, data_type, entry_point for account and contact)

- [ ] **Step 1: Run `discovering-source-schema` for Salesforce Account and Contact resources**
- [ ] **Step 2: Update plan.md pipeline inventory rows with discovered schema columns**
- [ ] **Step 3: Commit**

```bash
git add docs/design/pipelines/salesforce-account-contact.md plan.md
git commit -m "discover Salesforce schema and finalize pipeline inventory"
```

### Task 3: Generate dlt pipeline for Salesforce Account + Contact

**Requirement refs:** R-02@1

**Files:**
- Create: `ingestion/salesforce_pipeline.py`
- Create: `ingestion/.dlt/config.toml` (full configuration)

**Interfaces:**
- Consumes: Salesforce source connection (Task 1), discovered schema (Task 2)
- Produces: `ingestion/salesforce_pipeline.py` — runnable dlt pipeline with `replace` disposition

- [ ] **Step 1: Invoke `generating-dlt-pipeline` for Account and Contact resources**
  Generates `ingestion/salesforce_pipeline.py` with both resources, `replace` disposition, schema contracts.
- [ ] **Step 2: Commit**

```bash
git add ingestion/
git commit -m "generate Salesforce dlt pipeline"
```

### Task 4: Sandbox run — land bronze tables

**Requirement refs:** R-02@1

**Files:**
- Create: `ingestion/last-run-preview.md`

**Interfaces:**
- Consumes: `ingestion/salesforce_pipeline.py` (Task 3)
- Produces: bronze tables in ephemeral database

- [ ] **Step 1: Invoke `running-dlt-in-sandbox`**
  Runs the dlt pipeline against the ephemeral MotherDuck database.
- [ ] **Step 2: Render bronze preview**
  Runs bronze-preview render per conventions; writes `ingestion/last-run-preview.md`.
- [ ] **Step 3: Commit**

```bash
git add ingestion/last-run-preview.md
git commit -m "sandbox run: land Salesforce bronze tables"
```

### Task 5: Register bronze tables as dbt sources

**Requirement refs:** R-06@1

**Files:**
- Modify: `transformation/models/staging/_sources.yml`

**Interfaces:**
- Consumes: bronze table names from ephemeral run (Task 4)
- Produces: Updated `_sources.yml` with Salesforce source entries

- [ ] **Step 1: Invoke `registering-dbt-sources`**
  Registers the landed bronze tables as dbt sources in `transformation/models/staging/_sources.yml`.
- [ ] **Step 2: Run `dbt parse` to validate**
  ```bash
  dbt parse --project-dir transformation
  ```
- [ ] **Step 3: Commit**

```bash
git add transformation/models/staging/_sources.yml
git commit -m "register Salesforce bronze tables as dbt sources"
```

### Task 6: Generate silver dbt models (silver.salesforce_account, silver.salesforce_contact)

**Requirement refs:** R-03@1

**Files:**
- Create: `transformation/models/intermediate/silver_salesforce_account.sql`
- Create: `transformation/models/intermediate/silver_salesforce_contact.sql`
- Create: `transformation/models/intermediate/intermediate.yml` (model properties and tests)

**Interfaces:**
- Consumes: dbt source definitions (Task 5)
- Produces: Silver dbt model files (views) with column-level tests

- [ ] **Step 1: Invoke `generating-dbt-model` for `silver.salesforce_account`**
  Generate the SQL view — rename to snake_case, soft-delete filter (`is_deleted = false`), drop null IDs.
- [ ] **Step 2: Invoke `generating-dbt-model` for `silver.salesforce_contact`**
  Generate the SQL view — same conventions.
- [ ] **Step 3: Invoke `dbt-unit-testing` for silver models**
  Tests: unique + not-null on `account_id`/`contact_id`, accepted values for `is_deleted`.
- [ ] **Step 4: Commit**

```bash
git add transformation/models/intermediate/
git commit -m "generate silver Salesforce dbt models with tests"
```

### Task 7: Generate gold dbt marts (dim_account, dim_contact)

**Requirement refs:** R-04@1

**Files:**
- Create: `transformation/models/marts/dim_account.sql`
- Create: `transformation/models/marts/dim_contact.sql`
- Modify: `transformation/models/marts/marts.yml` (model properties and tests)

**Interfaces:**
- Consumes: silver model references (Task 6)
- Produces: Gold dbt model files (tables) with integrity tests

- [ ] **Step 1: Invoke `generating-dbt-model` for `dim_account`**
  Generate the table — all standard fields, control columns (`_loaded_at`, `_dbt_invocation_id`), unique key `account_id`.
- [ ] **Step 2: Invoke `generating-dbt-model` for `dim_contact`**
  Generate the table — all standard fields, preserve `account_id`, control columns, unique key `contact_id`.
- [ ] **Step 3: Invoke `dbt-unit-testing` for gold models**
  Tests: unique + not-null on keys, relationships (`account_id` → `dim_account`).
- [ ] **Step 4: Commit**

```bash
git add transformation/models/marts/
git commit -m "generate gold Salesforce dimension marts with tests"
```

### Task 8: Sandbox dbt build (full transformation)

**Requirement refs:** R-03@1, R-04@1

**Files:**
- No new files; runs existing dbt project against ephemeral

**Interfaces:**
- Consumes: all silver + gold model files (Tasks 6, 7), dbt sources (Task 5), bronze tables (Task 4)
- Produces: Materialized silver views and gold tables in ephemeral database

- [ ] **Step 1: Invoke `running-dbt-in-sandbox`**
  ```bash
  dbt build --project-dir transformation
  ```
- [ ] **Step 2: Commit (if any generated config changes)**

```bash
git add transformation/
git commit -m "sandbox dbt build: silver views and gold tables"
```

### Task 9: Generate MotherDuck Flight for weekly orchestration

**Requirement refs:** R-05@1

**Files:**
- Create: `orchestration/salesforce_weekly.Flight/config.json`
- Create: `orchestration/salesforce_weekly.Flight/flight.py`
- Create: `orchestration/salesforce_weekly.Flight/requirements.txt`

**Interfaces:**
- Consumes: dlt pipeline entry point, dbt project path, bronze table sources
- Produces: Committed Flight artifact at `orchestration/salesforce_weekly.Flight/`

- [ ] **Step 1: Invoke `generating-orchestration`**
  Author the Flight: `flight.py` (runs dlt pipeline → dbt build), `requirements.txt` (pinned), `config.json` (with `schedule_cron: "0 6 * * 1"`).
- [ ] **Step 2: Validate Flight**
  ```bash
  python3 validate-flight.py orchestration/salesforce_weekly.Flight --repo-root .
  ```
- [ ] **Step 3: Sandbox run of Flight**
  Invoke `running-orchestration-in-sandbox` against ephemeral database.
- [ ] **Step 4: Commit**

```bash
git add orchestration/
git commit -m "generate weekly Salesforce MotherDuck Flight"
```

### Task 10: Documentation

**Requirement refs:** R-02@1, R-03@1, R-04@1, R-05@1

- [ ] **Step 1: Invoke `documenting-dlt-pipelines`** for the Salesforce pipeline
- [ ] **Step 2: Invoke `documenting-dbt-models`** for silver + gold models
- [ ] **Step 3: Invoke `documenting-orchestration`** for the Flight
- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "add pipeline, model, and orchestration documentation"
```

## Execution evidence

*Task 1:* Salesforce source configured (`ingestion/.dlt/config.toml`), secrets created (`local_toml`), connection verified with live data — Account (2 rows) and Contact (2 rows) fetched. Committed dd7a6cd.

*Task 2:* Schema discovered from live Salesforce API:
- **Account**: 78 fields (standard + custom `__c` fields). Resource uses `write_disposition=merge` with incremental on `LastModifiedDate` (per verified source code), not `replace` as initially planned — pipeline inventory updated.
- **Contact**: ~55 fields (standard + custom `__c` fields). Resource uses `write_disposition=replace`. References Account via `AccountId`.
- Pipeline inventory rows updated with correct write_disposition and incremental_cursor.