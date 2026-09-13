# orchestration/salesforce_weekly.Flight/flight.py — dlt + dbt workload on MotherDuck.
#
# Runs in the MotherDuck Flight runtime: CPython 3.12, dependencies installed by
# uv from the sibling requirements.txt, /tmp writable, git on PATH, unrestricted
# network egress. This is NOT the Studio sandbox runtime — setup_environment()
# cannot initialize here, and the MotherDuck credential arrives as an injected
# MOTHERDUCK_TOKEN rather than from the credential broker.
#
# Every knob is read from Flight config so the program is adapted by setting
# config values, never by editing code. Each key MUST be declared in config.json:
# MD_RUN_FLIGHT rejects a per-run override for an undeclared key.
#
# The dlt pipeline runs inline (no git checkout) because the Flight runtime
# cannot authenticate with private GitHub repos. The dbt project files are
# emitted from the embedded variables below.
#
# To sync the embedded dbt project with the repository, update GIT_REVISION
# in config.json and regenerate from the checked-out files.
import io
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import urllib.request


def env(name: str) -> str:
    return os.environ[name]


IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def identifier(name: str) -> str:
    value = env(name)
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{name} must be a plain SQL identifier, got {value!r}")
    return value


def resolve_credential(key: str) -> str | None:
    """Resolve a credential from Flight secrets (namespaced) or config env var."""
    for candidate in (key, key.split("_", 1)[-1] if "_" in key else None):
        if candidate and candidate in os.environ:
            return os.environ[candidate]
    return None


# ── Embedded dbt project files ──────────────────────────────────────────
# These are generated from the repository at the GIT_REVISION below.
# Update them when the dbt models change.

DBT_PROJECT_YML = """\
name: motherduck_do
version: '1.0.0'
config-version: 2

profile: flight

model-paths: ['models']
macro-paths: ['macros']
test-paths: ['tests']
seed-paths: ['seeds']
snapshot-paths: ['snapshots']
target-path: target
clean-targets: ['target', 'dbt_packages']

models:
  motherduck_do:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table
"""

SOURCES_YML = """\
version: 2

sources:
  - name: salesforce
    description: Salesforce CRM objects ingested via dlt verified source.
    schema: main
    tables:
      - name: account
        description: Salesforce Account objects (one row per account).
      - name: contact
        description: Salesforce Contact objects (one row per contact).
"""

DARTS_YML = """\
version: 2

models:
  - name: dim_account
    description: "One row per Salesforce Account. Grain: one row per account_id."
    contracts:
      enforced: true
    columns:
      - name: account_id
        data_type: text
        tests: [not_null, unique]
      - name: account_name
        data_type: text
        tests: [not_null]
      - name: type
        data_type: text
      - name: industry
        data_type: text
      - name: annual_revenue
        data_type: double
      - name: number_of_employees
        data_type: integer
      - name: ownership
        data_type: text
      - name: ticker_symbol
        data_type: text
      - name: rating
        data_type: text
      - name: phone
        data_type: text
      - name: fax
        data_type: text
      - name: website
        data_type: text
      - name: billing_street
        data_type: text
      - name: billing_city
        data_type: text
      - name: billing_state
        data_type: text
      - name: billing_postal_code
        data_type: text
      - name: billing_country
        data_type: text
      - name: shipping_street
        data_type: text
      - name: shipping_city
        data_type: text
      - name: shipping_state
        data_type: text
      - name: shipping_postal_code
        data_type: text
      - name: shipping_country
        data_type: text
      - name: description
        data_type: text
      - name: owner_id
        data_type: text
      - name: created_date
        data_type: timestamp
      - name: last_modified_date
        data_type: timestamp
      - name: system_modstamp
        data_type: timestamp
      - name: is_partner
        data_type: boolean
      - name: is_customer_portal
        data_type: boolean
      - name: clean_status
        data_type: text
      - name: customer_priority
        data_type: text
      - name: sla
        data_type: text
      - name: active
        data_type: boolean
      - name: number_of_locations
        data_type: integer
      - name: upsell_opportunity
        data_type: text
      - name: sla_serial_number
        data_type: text
      - name: sla_expiration_date
        data_type: date
      - name: last_viewed_date
        data_type: timestamp
      - name: last_referenced_date
        data_type: timestamp
      - name: _loaded_at
        data_type: timestamp
        description: Timestamp when this row was loaded.
      - name: _dbt_invocation_id
        data_type: text
        description: Unique identifier for the dbt invocation.
      - name: _git_sha
        data_type: text
        description: Git SHA of the commit that produced this row.

  - name: dim_contact
    description: "One row per Salesforce Contact. Grain: one row per contact_id."
    contracts:
      enforced: true
    columns:
      - name: contact_id
        data_type: text
        tests: [not_null, unique]
      - name: salutation
        data_type: text
      - name: first_name
        data_type: text
      - name: last_name
        data_type: text
        tests: [not_null]
      - name: contact_name
        data_type: text
      - name: email
        data_type: text
      - name: title
        data_type: text
      - name: phone
        data_type: text
      - name: fax
        data_type: text
      - name: mobile_phone
        data_type: text
      - name: department
        data_type: text
      - name: lead_source
        data_type: text
      - name: birthdate
        data_type: date
      - name: mailing_street
        data_type: text
      - name: mailing_city
        data_type: text
      - name: mailing_state
        data_type: text
      - name: mailing_postal_code
        data_type: text
      - name: mailing_country
        data_type: text
      - name: mailing_state_code
        data_type: text
      - name: mailing_country_code
        data_type: text
      - name: other_street
        data_type: text
      - name: other_city
        data_type: text
      - name: other_state
        data_type: text
      - name: other_postal_code
        data_type: text
      - name: other_country
        data_type: text
      - name: other_country_code
        data_type: text
      - name: assistant_phone
        data_type: text
      - name: assistant_name
        data_type: text
      - name: home_phone
        data_type: text
      - name: description
        data_type: text
      - name: account_id
        data_type: text
        description: "Foreign key to dim_account. Nullable: contacts may not be linked to an account."
      - name: owner_id
        data_type: text
      - name: created_date
        data_type: timestamp
      - name: created_by_id
        data_type: text
      - name: last_modified_date
        data_type: timestamp
      - name: last_modified_by_id
        data_type: text
      - name: system_modstamp
        data_type: timestamp
      - name: is_email_bounced
        data_type: boolean
      - name: photo_url
        data_type: text
      - name: clean_status
        data_type: text
      - name: is_priority_record
        data_type: boolean
      - name: contact_level
        data_type: text
      - name: languages
        data_type: text
      - name: last_viewed_date
        data_type: timestamp
      - name: last_referenced_date
        data_type: timestamp
      - name: last_activity_date
        data_type: timestamp
      - name: _loaded_at
        data_type: timestamp
        description: Timestamp when this row was loaded.
      - name: _dbt_invocation_id
        data_type: text
        description: Unique identifier for the dbt invocation.
      - name: _git_sha
        data_type: text
        description: Git SHA of the commit that produced this row.
"""

GIT_SHA_MACRO = """\
{%- macro project_git_sha() -%}
  {{- var('git_sha', env_var('GIT_SHA', 'local')) | trim -}}
{%- endmacro -%}
"""

SILVER_ACCOUNT_SQL = """\
WITH source AS (
    SELECT
        id AS account_id,
        is_deleted,
        name AS account_name,
        type,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        billing_state_code,
        billing_country_code,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        shipping_country_code,
        shipping_state_code,
        phone,
        fax,
        account_number,
        website,
        photo_url,
        sic,
        industry,
        annual_revenue,
        number_of_employees,
        ownership,
        ticker_symbol,
        description,
        rating,
        owner_id,
        created_date,
        created_by_id,
        last_modified_date,
        last_modified_by_id,
        system_modstamp,
        is_partner,
        is_customer_portal,
        clean_status,
        customer_priority__c AS customer_priority,
        sla__c AS sla,
        active__c AS active,
        numberof_locations__c AS number_of_locations,
        upsell_opportunity__c AS upsell_opportunity,
        sla_serial_number__c AS sla_serial_number,
        sla_expiration_date__c AS sla_expiration_date,
        last_viewed_date,
        last_referenced_date,
        _dlt_load_id,
        _dlt_id
    FROM {{ source('salesforce', 'account') }}
    WHERE is_deleted = false
      AND id IS NOT NULL
)

SELECT * FROM source
"""

SILVER_CONTACT_SQL = """\
WITH source AS (
    SELECT
        id AS contact_id,
        is_deleted,
        account_id,
        last_name,
        first_name,
        salutation,
        name AS contact_name,
        email,
        title,
        phone,
        fax,
        mobile_phone,
        department,
        lead_source,
        birthdate,
        mailing_street,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_country,
        mailing_state_code,
        mailing_country_code,
        other_street,
        other_city,
        other_state,
        other_postal_code,
        other_country,
        other_country_code,
        assistant_phone,
        assistant_name,
        home_phone,
        description,
        owner_id,
        created_date,
        created_by_id,
        last_modified_date,
        last_modified_by_id,
        system_modstamp,
        is_email_bounced,
        photo_url,
        clean_status,
        is_priority_record,
        level__c AS contact_level,
        languages__c AS languages,
        last_viewed_date,
        last_referenced_date,
        last_activity_date,
        _dlt_load_id,
        _dlt_id
    FROM {{ source('salesforce', 'contact') }}
    WHERE is_deleted = false
      AND id IS NOT NULL
)

SELECT * FROM source
"""

DIM_ACCOUNT_SQL = """\
{{ config(materialized='table') }}

WITH final AS (
    SELECT
        account_id,
        account_name,
        type,
        industry,
        annual_revenue,
        number_of_employees,
        ownership,
        ticker_symbol,
        rating,
        phone,
        fax,
        website,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        description,
        owner_id,
        created_date,
        last_modified_date,
        system_modstamp,
        is_partner,
        is_customer_portal,
        clean_status,
        customer_priority,
        sla,
        active,
        number_of_locations,
        upsell_opportunity,
        sla_serial_number,
        sla_expiration_date,
        last_viewed_date,
        last_referenced_date,
        CURRENT_TIMESTAMP AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id,
        '{{ project_git_sha() }}' AS _git_sha
    FROM {{ ref('silver_salesforce_account') }}
)

SELECT * FROM final
"""

DIM_CONTACT_SQL = """\
{{ config(materialized='table') }}

WITH final AS (
    SELECT
        contact_id,
        salutation,
        first_name,
        last_name,
        contact_name,
        email,
        title,
        phone,
        fax,
        mobile_phone,
        department,
        lead_source,
        birthdate,
        mailing_street,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_country,
        mailing_state_code,
        mailing_country_code,
        other_street,
        other_city,
        other_state,
        other_postal_code,
        other_country,
        other_country_code,
        assistant_phone,
        assistant_name,
        home_phone,
        description,
        account_id,
        owner_id,
        created_date,
        created_by_id,
        last_modified_date,
        last_modified_by_id,
        system_modstamp,
        is_email_bounced,
        photo_url,
        clean_status,
        is_priority_record,
        contact_level,
        languages,
        last_viewed_date,
        last_referenced_date,
        last_activity_date,
        CURRENT_TIMESTAMP AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id,
        '{{ project_git_sha() }}' AS _git_sha
    FROM {{ ref('silver_salesforce_contact') }}
)

SELECT * FROM final
"""

# ── Embedded dlt pipeline ──────────────────────────────────────────────

DLT_PIPELINE_SRC = r"""import dlt
import os

def salesforce_source():
    from sources.salesforce import salesforce_source as _sf_source
    source = _sf_source.clone(name="salesforce", section="salesforce")().with_resources("account", "contact")

    source.account.apply_hints(
        write_disposition="merge",
        primary_key="Id",
        schema_contract={"columns": "freeze", "tables": "evolve", "data_type": "freeze"},
    )
    source.contact.apply_hints(
        write_disposition="replace",
        schema_contract={"columns": "freeze", "tables": "evolve", "data_type": "freeze"},
    )
    return source

pipeline = dlt.pipeline(
    pipeline_name="salesforce_account_contact",
    destination="motherduck",
    dataset_name="main",
)
"""


def write_dbt_project(root: str) -> str:
    """Write the embedded dbt project files to disk and return the project root."""
    project_dir = os.path.join(root, "transformation")
    models_staging = os.path.join(project_dir, "models", "staging")
    models_intermediate = os.path.join(project_dir, "models", "intermediate")
    models_marts = os.path.join(project_dir, "models", "marts")
    macros_dir = os.path.join(project_dir, "macros")
    tests_dir = os.path.join(project_dir, "tests")
    seeds_dir = os.path.join(project_dir, "seeds")
    snapshots_dir = os.path.join(project_dir, "snapshots")

    for d in [models_staging, models_intermediate, models_marts, macros_dir,
              tests_dir, seeds_dir, snapshots_dir]:
        os.makedirs(d, exist_ok=True)

    with open(os.path.join(project_dir, "dbt_project.yml"), "w") as f:
        f.write(DBT_PROJECT_YML)
    with open(os.path.join(models_staging, "_sources.yml"), "w") as f:
        f.write(SOURCES_YML)
    with open(os.path.join(models_marts, "marts.yml"), "w") as f:
        f.write(DARTS_YML)
    with open(os.path.join(macros_dir, "git_sha.sql"), "w") as f:
        f.write(GIT_SHA_MACRO)
    with open(os.path.join(models_intermediate, "silver_salesforce_account.sql"), "w") as f:
        f.write(SILVER_ACCOUNT_SQL)
    with open(os.path.join(models_intermediate, "silver_salesforce_contact.sql"), "w") as f:
        f.write(SILVER_CONTACT_SQL)
    with open(os.path.join(models_marts, "dim_account.sql"), "w") as f:
        f.write(DIM_ACCOUNT_SQL)
    with open(os.path.join(models_marts, "dim_contact.sql"), "w") as f:
        f.write(DIM_CONTACT_SQL)

    return project_dir


def write_pipeline_code(root: str) -> str:
    """Write the dlt pipeline script to disk and return the path."""
    pipeline_root = os.path.join(root, "ingestion")
    os.makedirs(os.path.join(pipeline_root, "sources"), exist_ok=True)
    with open(os.path.join(pipeline_root, "salesforce_pipeline.py"), "w") as f:
        f.write(DLT_PIPELINE_SRC)
    return pipeline_root


def write_profile(directory: str, database: str, schema: str) -> None:
    with open(os.path.join(directory, "profiles.yml"), "w") as handle:
        handle.write(
            "flight:\n"
            "  target: prod\n"
            "  outputs:\n"
            "    prod:\n"
            "      type: duckdb\n"
            f'      path: "md:{database}"\n'
            f"      schema: {schema}\n")


def run_dlt(workdir: str, database: str) -> bool:
    """Run the dlt Salesforce pipeline.

    Returns True on success, False when credentials are unavailable (sandbox).
    Exits the Flight on a load failure.
    """
    user = resolve_credential("salesforce_SALESFORCE_USER_NAME")
    password = resolve_credential("salesforce_SALESFORCE_PASSWORD")
    token = resolve_credential("salesforce_SALESFORCE_SECURITY_TOKEN")

    if not all([user, password, token]):
        print("dlt SKIPPED: Salesforce credentials not available in this environment")
        return False

    os.environ["SOURCES__SALESFORCE__CREDENTIALS__USER_NAME"] = user
    os.environ["SOURCES__SALESFORCE__CREDENTIALS__PASSWORD"] = password
    os.environ["SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN"] = token
    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE"] = database

    sys.path.insert(0, os.path.join(workdir, "ingestion"))

    import importlib.util
    pipeline_path = os.path.join(workdir, "ingestion", "salesforce_pipeline.py")
    pipeline_spec = importlib.util.spec_from_file_location("pipeline", pipeline_path)
    pipeline_mod = importlib.util.module_from_spec(pipeline_spec)
    pipeline_spec.loader.exec_module(pipeline_mod)

    load_info = pipeline_mod.pipeline.run(
        pipeline_mod.salesforce_source(),
        write_disposition="merge",
        loader_file_format="parquet",
    )
    print("dlt load:", load_info)

    unfinished = [pkg for pkg in load_info.load_packages
                  if pkg.state != "loaded"]
    if load_info.has_failed_jobs or unfinished:
        print("dlt FAILED:",
              "failed jobs present;" if load_info.has_failed_jobs else "",
              "packages not loaded:" if unfinished else "",
              ", ".join(pkg.load_id for pkg in unfinished), file=sys.stderr)
        sys.exit(1)
    print("dlt: load OK")
    return True


def run_dbt(database: str, schema: str, selector: str, project_dir: str) -> None:
    profiles = os.path.join(tempfile.mkdtemp(), "profiles")
    os.makedirs(profiles)
    write_profile(profiles, database, schema)

    if not os.path.exists(os.path.join(project_dir, "dbt_project.yml")):
        raise FileNotFoundError(
            f"no dbt_project.yml under {project_dir}")

    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(
        ["build", "--select", selector,
         "--project-dir", project_dir, "--profiles-dir", profiles, "--profile", "flight"])

    run_results = os.path.join(project_dir, "target", "run_results.json")
    if os.path.exists(run_results):
        with open(run_results) as handle:
            summary = json.load(handle)
        print("dbt nodes:", len(summary.get("results", [])),
              "elapsed:", summary.get("elapsed_time"))

    if not result.success:
        failed = [node.node.unique_id for node in (result.result or [])
                  if str(node.status) not in ("success", "pass")]
        print("dbt FAILED:", ", ".join(failed) or "see logs", file=sys.stderr)
        sys.exit(1)
    print("dbt build succeeded for selector:", selector)


def main() -> None:
    os.environ["HOME"] = "/tmp"
    database = identifier("DESTINATION_DATABASE")
    schema = env("DBT_SCHEMA")
    selector = env("DBT_SELECT")
    os.environ["GIT_SHA"] = env("GIT_REVISION")

    import duckdb
    con = duckdb.connect("md:")
    con.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    con.close()

    workdir = tempfile.mkdtemp()
    original_cwd = os.getcwd()

    # Emit embedded dbt project files to disk
    project_dir = write_dbt_project(workdir)
    write_pipeline_code(workdir)
    print("dbt project written to:", project_dir)

    # Step 1: dlt ingestion (skipped gracefully when credentials absent)
    run_dlt(workdir, database)

    # Step 2: dbt transformation
    run_dbt(database, schema, selector, project_dir)

    os.chdir(original_cwd)
    print("Salesforce weekly pipeline: complete")


if __name__ == "__main__":
    main()