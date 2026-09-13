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
# The repo is public so we clone it directly — no embedded files.
import json
import os
import re
import subprocess
import sys
import tempfile


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


REPO_URL = "https://github.com/pradipsodha-acceleratedata/motherduck-do.git"


# Simplified dlt pipeline for Flight runtime (no vibedata.dlt.duckdb dependency).
# Written into the cloned repo's ingestion/ dir so it resolves the vendored
# sources.salesforce package from there.
FLIGHT_PIPELINE_SRC = """\
import dlt
import os
import sys

# __file__ is in the ingestion/ dir, so no extra "ingestion" suffix needed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sources.salesforce import salesforce_source as _sf_source

def make_source():
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


def clone_repo(workdir: str, revision: str) -> str:
    """Clone the public repo at the given revision and return the checkout path."""
    dest = os.path.join(workdir, "repo")
    subprocess.run(["git", "clone", "--depth", "1", REPO_URL, dest],
                   check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", dest, "fetch", "--depth", "1", "origin", revision],
                   check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", dest, "checkout", revision],
                   check=True, capture_output=True, text=True)
    print("cloned repo at revision:", revision)
    return dest


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


def run_dlt(repo_root: str, database: str) -> bool:
    """Run the dlt Salesforce pipeline from the Flight-specific inline script.

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

    # Add ingestion/ to sys.path so `from sources.salesforce import ...` resolves
    sys.path.insert(0, os.path.join(repo_root, "ingestion"))

    # Write the Flight-specific pipeline script and exec it
    pipeline_dir = os.path.join(repo_root, "ingestion")
    flight_pipeline_path = os.path.join(pipeline_dir, "_flight_pipeline.py")
    with open(flight_pipeline_path, "w") as f:
        f.write(FLIGHT_PIPELINE_SRC)

    import importlib.util
    pipeline_spec = importlib.util.spec_from_file_location("_flight_pipeline", flight_pipeline_path)
    pipeline_mod = importlib.util.module_from_spec(pipeline_spec)
    pipeline_spec.loader.exec_module(pipeline_mod)

    load_info = pipeline_mod.pipeline.run(
        pipeline_mod.make_source(),
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
    revision = env("GIT_REVISION")
    os.environ["GIT_SHA"] = revision

    import duckdb
    con = duckdb.connect("md:")
    con.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    con.close()

    workdir = tempfile.mkdtemp()
    original_cwd = os.getcwd()

    # Clone the public repo
    repo_root = clone_repo(workdir, revision)
    project_dir = os.path.join(repo_root, "transformation")
    print("dbt project dir:", project_dir)

    # Step 1: dlt ingestion (skipped gracefully when credentials absent)
    run_dlt(repo_root, database)

    # Step 2: dbt transformation
    run_dbt(database, schema, selector, project_dir)

    os.chdir(original_cwd)
    print("Salesforce weekly pipeline: complete")


if __name__ == "__main__":
    main()