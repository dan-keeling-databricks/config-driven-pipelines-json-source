# Databricks notebook source
# MAGIC %md
# MAGIC # Config-driven DAB factory
# MAGIC
# MAGIC This notebook reads `config.json`, fills the templates in `templates/`, and
# MAGIC recreates the matching bundle under `generated/`.

# COMMAND ----------

from __future__ import annotations

import json
import os
import pprint
import re
import shutil
from pathlib import Path
from typing import Any


IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
TOKEN = re.compile(r"\{\{([A-Z][A-Z0-9_]*)\}\}")


def notebook_root() -> Path:
    """Locate the Git folder, with an optional notebook widget override."""
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


def render(template: str, values: dict[str, str]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{{" + key + "}}", value)

    unresolved = sorted(set(TOKEN.findall(rendered)))
    if unresolved:
        raise ValueError(f"Template has unresolved values: {', '.join(unresolved)}")
    return rendered


def yaml_string(value: str) -> str:
    """A JSON string is also a valid YAML string scalar."""
    return json.dumps(value)


def python_literal(value: Any) -> str:
    return pprint.pformat(value, width=100, sort_dicts=True)


def validate_identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must match {IDENTIFIER.pattern!r}; got {value!r}")
    return value


def require_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return value


def validate_config(config: dict[str, Any]) -> None:
    bundle = require_mapping(config.get("bundle"), "bundle")
    validate_identifier(bundle.get("name"), "bundle.name")
    validate_identifier(bundle.get("catalog"), "bundle.catalog")
    validate_identifier(bundle.get("source_volume"), "bundle.source_volume")

    targets = require_mapping(config.get("targets"), "targets")
    for target_name in ("dev", "prod"):
        target = require_mapping(targets.get(target_name), f"targets.{target_name}")
        validate_identifier(target.get("schema"), f"targets.{target_name}.schema")
        if target.get("schedule_pause_status") not in {"PAUSED", "UNPAUSED"}:
            raise ValueError(
                f"targets.{target_name}.schedule_pause_status must be PAUSED or UNPAUSED"
            )

    datasets = config.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ValueError("datasets must be a non-empty JSON array")

    names: set[str] = set()
    for index, dataset in enumerate(datasets):
        dataset = require_mapping(dataset, f"datasets[{index}]")
        name = validate_identifier(dataset.get("name"), f"datasets[{index}].name")
        if name in names:
            raise ValueError(f"Duplicate dataset name: {name}")
        names.add(name)

        source = require_mapping(dataset.get("source"), f"datasets[{index}].source")
        if source.get("format") not in {"json", "csv", "parquet", "avro", "orc", "xml"}:
            raise ValueError(f"Unsupported source format for {name}: {source.get('format')!r}")
        if not isinstance(source.get("path"), str) or not source["path"].strip():
            raise ValueError(f"datasets[{index}].source.path must be a non-empty string")

        silver = require_mapping(dataset.get("silver"), f"datasets[{index}].silver")
        if not isinstance(silver.get("select"), list) or not silver["select"]:
            raise ValueError(f"datasets[{index}].silver.select must be a non-empty array")
        expectations = require_mapping(
            silver.get("expectations", {}), f"datasets[{index}].silver.expectations"
        )
        for action in ("warn", "drop", "fail"):
            require_mapping(expectations.get(action, {}), f"{name}.expectations.{action}")

        schedule = require_mapping(dataset.get("schedule"), f"datasets[{index}].schedule")
        if not schedule.get("quartz_cron_expression") or not schedule.get("timezone_id"):
            raise ValueError(f"Dataset {name} requires a cron expression and timezone")


def expectation_decorators(expectations: dict[str, dict[str, str]]) -> str:
    decorators = {
        "warn": "expect_all",
        "drop": "expect_all_or_drop",
        "fail": "expect_all_or_fail",
    }
    lines: list[str] = []
    for action, decorator in decorators.items():
        rules = expectations.get(action, {})
        if rules:
            lines.append(f"@dp.{decorator}({python_literal(rules)})")
    return "\n".join(lines)


def write_generated_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def generate(repo_root: Path) -> Path:
    config_path = repo_root / "config.json"
    template_root = repo_root / "templates"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    validate_config(config)

    bundle = config["bundle"]
    targets = config["targets"]
    bundle_name = bundle["name"]

    generated_root = (repo_root / "generated").resolve()
    output_root = (generated_root / bundle_name).resolve()
    if output_root.parent != generated_root:
        raise ValueError(f"Refusing to generate outside {generated_root}")

    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)

    template_names = ("databricks.yml", "pipeline.yml", "job.yml", "dataset.py")
    templates = {
        name: (template_root / f"{name}.tmpl").read_text(encoding="utf-8")
        for name in template_names
    }

    databricks_yml = render(
        templates["databricks.yml"],
        {
            "BUNDLE_NAME": bundle_name,
            "CATALOG": yaml_string(bundle["catalog"]),
            "SOURCE_VOLUME": yaml_string(bundle["source_volume"]),
            "DEV_SCHEMA": yaml_string(targets["dev"]["schema"]),
            "DEV_PAUSE_STATUS": targets["dev"]["schedule_pause_status"],
            "PROD_SCHEMA": yaml_string(targets["prod"]["schema"]),
            "PROD_PAUSE_STATUS": targets["prod"]["schedule_pause_status"],
        },
    )
    write_generated_file(output_root / "databricks.yml", databricks_yml)

    generated_files = ["databricks.yml"]
    for dataset in config["datasets"]:
        name = dataset["name"]
        pipeline_key = f"{name}_pipeline"
        source = dataset["source"]
        silver = dataset["silver"]
        schedule = dataset["schedule"]

        pipeline_yml = render(
            templates["pipeline.yml"],
            {
                "DATASET_NAME": name,
                "PIPELINE_KEY": pipeline_key,
                "SOURCE_SUBPATH": source["path"].strip("/"),
                "SOURCE_FORMAT": source["format"],
            },
        )
        pipeline_path = Path("resources") / f"{name}.pipeline.yml"
        write_generated_file(output_root / pipeline_path, pipeline_yml)
        generated_files.append(pipeline_path.as_posix())

        job_yml = render(
            templates["job.yml"],
            {
                "DATASET_NAME": name,
                "PIPELINE_KEY": pipeline_key,
                "CRON_EXPRESSION": yaml_string(schedule["quartz_cron_expression"]),
                "TIMEZONE_ID": yaml_string(schedule["timezone_id"]),
            },
        )
        job_path = Path("resources") / f"{name}.job.yml"
        write_generated_file(output_root / job_path, job_yml)
        generated_files.append(job_path.as_posix())

        source_options = dict(source.get("options", {}))
        if source.get("schema_hints"):
            source_options["cloudFiles.schemaHints"] = source["schema_hints"]
        source_options.setdefault("rescuedDataColumn", "_rescued_data")

        dataset_py = render(
            templates["dataset.py"],
            {
                "DATASET_NAME": name,
                "DESCRIPTION_REPR": repr(dataset.get("description", name)),
                "SOURCE_OPTIONS_REPR": python_literal(source_options),
                "SILVER_SELECT_REPR": python_literal(silver["select"]),
                "EXPECTATION_DECORATORS": expectation_decorators(
                    silver.get("expectations", {})
                ),
            },
        )
        source_path = (
            Path("src") / "pipelines" / name / "transformations" / f"{name}.py"
        )
        write_generated_file(output_root / source_path, dataset_py)
        generated_files.append(source_path.as_posix())

    manifest = {
        "generated_by": "factory.py",
        "bundle_name": bundle_name,
        "datasets": [dataset["name"] for dataset in config["datasets"]],
        "files": sorted(generated_files),
    }
    write_generated_file(
        output_root / "generated-manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True),
    )
    return output_root


# COMMAND ----------

REPO_ROOT = notebook_root()
GENERATED_BUNDLE = generate(REPO_ROOT)
print(f"Generated bundle: {GENERATED_BUNDLE}")
