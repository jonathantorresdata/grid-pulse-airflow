"""Paths and shared settings. GRIDPULSE_HOME points to a checkout of spain-grid-pulse."""

import os
from datetime import date
from pathlib import Path

from airflow.sdk import Asset

GRIDPULSE_HOME = Path(os.environ.get("GRIDPULSE_HOME", "/opt/airflow/spain-grid-pulse"))
BRONZE_DIR = GRIDPULSE_HOME / "data" / "bronze"
DBT_DIR = GRIDPULSE_HOME / "dbt"
DUCKDB_PATH = GRIDPULSE_HOME / "data" / "gridpulse.duckdb"
SITE_DIR = GRIDPULSE_HOME / "site"
HISTORY_START = date.fromisoformat(os.environ.get("GRIDPULSE_START", "2023-01-01"))

# Data-aware scheduling: the extract DAG updates BRONZE; the transform DAG runs when it changes.
BRONZE = Asset("gridpulse://bronze")
GOLD = Asset("gridpulse://gold")
