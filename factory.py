# Databricks notebook source
# MAGIC %md
# MAGIC # Config-driven DAB factory
# MAGIC
# MAGIC This notebook reads `config.json`, fills the code templates in `templates/`,
# MAGIC and writes a complete Databricks bundle under `generated/`.

# COMMAND ----------

import json
import pprint
import re
import shutil
from pathlib import Path


# The notebook is stored at the root of the Databricks Git folder. Databricks
# notebooks do not define __file__, while local Python runs do.
REPO_ROOT = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
TEMPLATE_ROOT = REPO_ROOT / "templates"

# Dataset and bundle names become Python names, folder names, and DAB resource keys.
VALID_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
PLACEHOLDER = re.compile(r"\{\{([A-Z][A-Z0-9_]*)\}\}")


def render(template_name: str, values: dict[str, str]) -> str:
    """Load a template and replace its `{{PLACEHOLDER}}` values."""
    template_path = TEMPLATE_ROOT / f"{template_name}.tmpl"
    text = template_path.read_text(encoding="utf-8")

    for name, value in values.items():
        text = text.replace(f"{{{{{name}}}}}", value)

    unresolved = sorted(set(PLACEHOLDER.findall(text)))
    if unresolved:
        raise ValueError(
            f"{template_path.name} has unresolved placeholders: {', '.join(unresolved)}"
        )
    return text


def write(path: Path, content: str) -> None:
    """Create parent folders and write one generated file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def python_literal(value: object) -> str:
    """Format lists and dictionaries cleanly in generated Python files."""
    return pprint.pformat(value, width=100, sort_dicts=True)


def expectations_as_decorators(expectations: dict) -> str:
    """Convert warn/drop/fail rules into SDP expectation decorators."""
    decorators = {
        "warn": "expect_all",
        "drop": "expect_all_or_drop",
        "fail": "expect_all_or_fail",
    }
    return "\n".join(
        f"@dp.{decorator}({python_literal(expectations[action])})"
        for action, decorator in decorators.items()
        if expectations.get(action)
    )


def load_config() -> dict:
    """Read config.json and perform the few checks needed for safe generation."""
    config = json.loads((REPO_ROOT / "config.json").read_text(encoding="utf-8"))

    missing_sections = {"bundle", "targets", "datasets"} - config.keys()
    if missing_sections:
        raise ValueError(f"Missing config sections: {sorted(missing_sections)}")
    if not config["datasets"]:
        raise ValueError("At least one dataset is required")

    names = [config["bundle"].get("name", "")]
    names.extend(dataset.get("name", "") for dataset in config["datasets"])
    invalid_names = [name for name in names if not VALID_NAME.fullmatch(name)]
    if invalid_names:
        raise ValueError(f"Invalid bundle or dataset names: {invalid_names}")

    dataset_names = names[1:]
    if len(dataset_names) != len(set(dataset_names)):
        raise ValueError("Dataset names must be unique")

    return config


def generate_dataset(bundle_root: Path, dataset: dict) -> None:
    """Generate one pipeline, its schedule, and its Bronze/Silver Python code."""
    name = dataset["name"]
    source = dataset["source"]
    silver = dataset["silver"]
    schedule = dataset["schedule"]
    pipeline_key = f"{name}_pipeline"

    write(
        bundle_root / f"resources/{name}.pipeline.yml",
        render(
            "pipeline.yml",
            {
                "DATASET_NAME": name,
                "PIPELINE_KEY": pipeline_key,
                "SOURCE_SUBPATH": source["path"].strip("/"),
                "SOURCE_FORMAT": source["format"],
            },
        ),
    )

    # Pipelines do not contain schedules, so a Lakeflow Job triggers each pipeline.
    write(
        bundle_root / f"resources/{name}.job.yml",
        render(
            "job.yml",
            {
                "DATASET_NAME": name,
                "PIPELINE_KEY": pipeline_key,
                "CRON_EXPRESSION": json.dumps(schedule["quartz_cron_expression"]),
                "TIMEZONE_ID": json.dumps(schedule["timezone_id"]),
            },
        ),
    )

    # These options are shared by every generated Auto Loader Bronze table.
    source_options = dict(source.get("options", {}))
    if source.get("schema_hints"):
        source_options["cloudFiles.schemaHints"] = source["schema_hints"]
    source_options.setdefault("rescuedDataColumn", "_rescued_data")

    write(
        bundle_root / f"src/pipelines/{name}/transformations/{name}.py",
        render(
            "dataset.py",
            {
                "DATASET_NAME": name,
                "DESCRIPTION_REPR": repr(dataset.get("description", name)),
                "SOURCE_OPTIONS_REPR": python_literal(source_options),
                "SILVER_SELECT_REPR": python_literal(silver["select"]),
                "EXPECTATION_DECORATORS": expectations_as_decorators(
                    silver.get("expectations", {})
                ),
            },
        ),
    )


def generate_bundle() -> Path:
    """Recreate the generated bundle from config.json."""
    config = load_config()
    bundle = config["bundle"]
    targets = config["targets"]
    bundle_root = REPO_ROOT / "generated" / bundle["name"]

    # This folder contains generated output only, so each run starts clean.
    if bundle_root.exists():
        shutil.rmtree(bundle_root)

    # json.dumps quotes strings safely; JSON strings are also valid YAML strings.
    write(
        bundle_root / "databricks.yml",
        render(
            "databricks.yml",
            {
                "BUNDLE_NAME": bundle["name"],
                "CATALOG": json.dumps(bundle["catalog"]),
                "SOURCE_SCHEMA": json.dumps(bundle["source_schema"]),
                "SOURCE_VOLUME": json.dumps(bundle["source_volume"]),
                "DEV_SCHEMA": json.dumps(targets["dev"]["schema"]),
                "DEV_PAUSE_STATUS": targets["dev"]["schedule_pause_status"],
                "PROD_SCHEMA": json.dumps(targets["prod"]["schema"]),
                "PROD_PAUSE_STATUS": targets["prod"]["schedule_pause_status"],
            },
        ),
    )

    for dataset in config["datasets"]:
        generate_dataset(bundle_root, dataset)

    return bundle_root


# COMMAND ----------

GENERATED_BUNDLE = generate_bundle()
print(f"Generated bundle: {GENERATED_BUNDLE}")
