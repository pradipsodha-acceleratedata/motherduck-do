# ingestion/salesforce_pipeline.py  —  MotherDuck domain (VD_DOMAIN_DATA_PLATFORM: motherduck); flat per `dlt init`, run from the ingestion root
#
# Runs in the Studio-local intent venv with the vibedata-dlt-duckdb-studio
# distribution (vibedata.dlt.duckdb import package). NEVER put credentials or
# DESTINATION__* config in this file — setup_environment() resolves them: source
# secrets from the Domain's bound Secret Store, and for MotherDuck both the
# acting user's brokered credential and the sandbox database, mapped onto dlt's
# own config names. It must run before the pipeline is constructed.
#
# destination="motherduck" is load-bearing: any other destination writes a local
# file and still reports LOAD OK (sandbox-dlt-motherduck.md).
#
# RUN CONTRACT — this file is executed only by the `running-dlt-in-sandbox`
# skill: your next tool call after committing this file is
# invoke_skill(name="running-dlt-in-sandbox"), never `python` on this file.
# A direct run skips the destination-identity assertion, the Tier-1 bronze
# gate and the preview, and leaves the first-run limits with no full load.
import dlt
from vibedata.dlt.duckdb import setup_environment, finalize

from sources.salesforce import salesforce_source   # vendored verified connector, used as-is

# ── Identity ──────────────────────────────────────────────────────────────
# CONNECTION_NAME: which source-system connection this pipeline reads — the
# dlt source section [sources.<CONNECTION_NAME>] where config/secrets
# resolve. Several pipelines may share one connection.
CONNECTION_NAME = "salesforce"
# PIPELINE_NAME: the run/job identity for dlt state — NOT the connection.
# Keep it distinct per job so incremental state never collides.
PIPELINE_NAME = "salesforce_account_contact"

setup_environment()

pipeline = dlt.pipeline(
    pipeline_name=PIPELINE_NAME,
    destination="motherduck",                   # native md: destination; the database comes from config, never from this file
    dataset_name="main",                        # dlt default landing schema for bronze
)

# The CONNECTION is the dlt source section: clone the connector into
# [sources.salesforce] so config/secrets resolve from that section.
source = salesforce_source.clone(name=CONNECTION_NAME, section=CONNECTION_NAME)().with_resources("account", "contact")

# Schema contract: columns frozen after first load, tables evolve until all resources land
source.account.apply_hints(
    write_disposition="merge",                  # incremental on LastModifiedDate per verified source
    primary_key="Id",                            # why: merge key for incremental upsert
    schema_contract={"columns": "freeze", "tables": "evolve", "data_type": "freeze"},
)
source.contact.apply_hints(
    write_disposition="replace",                # full replace per verified source
    schema_contract={"columns": "freeze", "tables": "evolve", "data_type": "freeze"},
)

# First-run safety: limit to one yield per resource
# source.account.add_limit(1)
# source.contact.add_limit(1)

try:
    load_info = pipeline.run(source)
except Exception as exc:
    finalize(pipeline, error_message=str(exc))   # audit the failed run, then re-raise
    raise
print(load_info)
print("audit:", finalize(pipeline))             # audit only (MotherDuck writes land directly)