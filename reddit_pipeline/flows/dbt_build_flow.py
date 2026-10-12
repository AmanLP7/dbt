"""Run the dbt build for the Reddit pipeline as its own Prefect flow.

Extraction and transformation are kept apart: `reddit_extract_flow` owns the
Reddit API calls and the raw-table loads, this flow owns everything dbt. The
extract flow triggers this one when it completes, and this one can also be run
on its own when only the models need rebuilding. Run it from the repo root:

    python -m flows.dbt_build_flow
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from databricks.sdk.core import Config
from dotenv import load_dotenv
from prefect import flow, task

# Populate os.environ from .env so the dbt subprocess below inherits the same
# DATABRICKS_HOSTNAME / DATABRICKS_HTTP_PATH the extractors connect with.
load_dotenv()

# Repo root: this module lives in flows/, a sibling of dbt_project.yml.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Repo-owned dbt profile; see profiles/profiles.yml for why it is not in ~/.dbt.
PROFILES_DIR = PROJECT_ROOT / "profiles"

# `dbt build` with no --select builds and tests every model in the project.
DBT_ARGS = ("build",)

# Env vars the dbt step needs; the extractors require the same two.
_DBT_REQUIRED_ENV = ("DATABRICKS_HOSTNAME", "DATABRICKS_HTTP_PATH")


def _dbt_env() -> dict[str, str]:
    """Build the environment for the dbt subprocess.

    Adds `DATABRICKS_TOKEN`, obtained from the Databricks CLI profile the
    extractors already authenticate with, so dbt and the extractors share one
    credential source. Passing a *token* rather than letting dbt run its own
    `auth_type: oauth` flow is deliberate: that flow is a browser U2M login and
    cannot complete unattended.
    """
    missing = [var for var in _DBT_REQUIRED_ENV if not os.getenv(var)]
    if missing:
        raise RuntimeError(f"Missing environment variables for the dbt step: {missing}. Configure them in .env.")

    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    headers = Config(profile=profile).authenticate()
    token = headers["Authorization"].split(" ", 1)[1]
    return {**os.environ, "DATABRICKS_TOKEN": token}


@task(name="run-dbt-build", retries=3, retry_delay_seconds=[10, 60, 300], retry_jitter_factor=0.5, log_prints=True)
def run_dbt_build() -> None:
    """Build every dbt model in the project and run its tests."""
    dbt = shutil.which("dbt")
    if dbt is None:
        raise RuntimeError(
            "`dbt` was not found on PATH. Install the pipeline requirements "
            "(`pip install -r requirements.txt`) into the environment running this flow."
        )

    subprocess.run(
        [dbt, *DBT_ARGS, "--profiles-dir", str(PROFILES_DIR)],
        cwd=PROJECT_ROOT,
        env=_dbt_env(),
        check=True,
    )


@flow(name="dbt-build", log_prints=True)
def dbt_build_flow() -> None:
    """Build the dbt models for the pipeline.

    Runs on its own, or as the step `reddit_extract_flow` triggers once
    extraction has finished and the raw tables hold the new rows.
    """
    run_dbt_build()


if __name__ == "__main__":
    dbt_build_flow()
