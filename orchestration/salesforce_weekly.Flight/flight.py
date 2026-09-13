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
# RUN CONTRACT — this file is deployed and executed only by
# `running-orchestration-in-sandbox`, which passes it as the Flight's source_code.
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


def checkout(repo: str, revision: str, destination: str) -> None:
    subprocess.run(["git", "init", "--quiet", destination], check=True)
    subprocess.run(["git", "-C", destination, "remote", "add", "origin", repo], check=True)
    subprocess.run(["git", "-C", destination, "fetch", "--quiet", "--depth", "1",
                    "origin", revision], check=True)
    subprocess.run(["git", "-C", destination, "checkout", "--quiet", "FETCH_HEAD"], check=True)


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


def run_dlt(database: str) -> None:
    """Run the dlt Salesforce pipeline from the checked-out repository."""
    # Salesforce credentials arrive as Flight secrets under namespaced env vars.
    # Set them in dlt's env-var convention (__ for nesting) so the connector
    # reads them without a secrets.toml file.
    os.environ["SOURCES__SALESFORCE__CREDENTIALS__USER_NAME"] = (
        env("salesforce_SALESFORCE_USER_NAME"))
    os.environ["SOURCES__SALESFORCE__CREDENTIALS__PASSWORD"] = (
        env("salesforce_SALESFORCE_PASSWORD"))
    os.environ["SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN"] = (
        env("salesforce_SALESFORCE_SECURITY_TOKEN"))

    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE"] = database

    sys.path.insert(0, os.path.join(os.getcwd(), "ingestion"))

    import importlib.util
    pipeline_path = os.path.join(os.getcwd(), "ingestion", "salesforce_pipeline.py")
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


def run_dbt(database: str, schema: str, selector: str, project_dir: str) -> None:
    """Run dbt build from the checked-out repository."""
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

    # Create the database in MotherDuck if it does not exist.
    import duckdb

    con = duckdb.connect("md:")
    con.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    con.close()

    workdir = tempfile.mkdtemp()
    original_cwd = os.getcwd()
    checkout(env("GIT_REPO"), revision, workdir)
    print("checked out revision:", revision)

    project_subdir = env("DBT_PROJECT_SUBDIR")
    project_dir = os.path.normpath(os.path.join(workdir, project_subdir))
    if not os.path.exists(os.path.join(project_dir, "dbt_project.yml")):
        raise FileNotFoundError(
            f"no dbt_project.yml under DBT_PROJECT_SUBDIR={project_subdir!r}")

    # Work from the checkout root so relative imports (ingestion/) resolve.
    os.chdir(workdir)

    # Step 1: run dlt ingestion
    run_dlt(database)

    # Step 2: run dbt transformation
    run_dbt(database, schema, selector, project_dir)

    os.chdir(original_cwd)
    print("Salesforce weekly pipeline: complete")


if __name__ == "__main__":
    main()