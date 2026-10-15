# Databricks notebook source
# MAGIC %md
# MAGIC # Upload mock JSON source data
# MAGIC
# MAGIC Run this notebook from the cloned Databricks Git folder. It creates the
# MAGIC configured schema and managed volume if required, then copies the checked-in
# MAGIC JSON batches into the source-system landing folders.

# COMMAND ----------

from __future__ import annotations

import os
import shutil
from pathlib import Path


CATALOG = "classic_stable_dankeeling_catalog"
SCHEMA = "config-driven-pipelines-demo"
DEV_TARGET_SCHEMA = "config-driven-pipelines-demo-dev"
VOLUME = "landing"
VOLUME_ROOT = Path("/Volumes") / CATALOG / SCHEMA / VOLUME


def sql_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def notebook_root() -> Path:
    configured_root = os.environ.get("DAB_FACTORY_ROOT", "").strip()

    try:
        dbutils.widgets.text("repo_root", "", "Repository root (optional)")  # type: ignore[name-defined]
        configured_root = dbutils.widgets.get("repo_root").strip() or configured_root  # type: ignore[name-defined]
    except NameError:
        pass

    if configured_root:
        return Path(configured_root).expanduser().resolve()

    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd().resolve()


# COMMAND ----------

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(CATALOG)}.{sql_identifier(SCHEMA)} "
    "COMMENT 'Config-driven JSON ingestion demo'"
)
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(CATALOG)}."
    f"{sql_identifier(DEV_TARGET_SCHEMA)} "
    "COMMENT 'Development output tables for the config-driven JSON ingestion demo'"
)
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {sql_identifier(CATALOG)}.{sql_identifier(SCHEMA)}."
    f"{sql_identifier(VOLUME)} COMMENT 'Landing files for the JSON source-system demo'"
)

source_root = notebook_root() / "mock_data"
if not source_root.is_dir():
    raise FileNotFoundError(f"Mock data folder not found: {source_root}")

copied_files: list[Path] = []
for source_file in sorted(source_root.glob("*/*.json")):
    relative_path = source_file.relative_to(source_root)
    destination = VOLUME_ROOT / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_file, destination)
    copied_files.append(destination)

print(f"Copied {len(copied_files)} JSON files to {VOLUME_ROOT}")
for copied_file in copied_files:
    print(copied_file)
