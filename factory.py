# Databricks notebook source
# MAGIC %md
# MAGIC # Config-driven pipeline factory
# MAGIC
# MAGIC This notebook turns `config.json` into a complete Databricks bundle.
# MAGIC
# MAGIC It demonstrates a deliberately simple factory pattern:
# MAGIC
# MAGIC 1. Read the source-system and dataset configuration.
# MAGIC 2. Fill a small set of pre-built templates.
# MAGIC 3. Generate one Bronze/Silver pipeline and one scheduled Job per dataset.
# MAGIC 4. Write the deployable bundle under `generated/`.
# MAGIC
# MAGIC The notebook prints progress as each part of the bundle is created.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Locate the Git folder
# MAGIC
# MAGIC The notebook is stored at the root of the Databricks Git folder, alongside
# MAGIC `config.json` and the `templates/` directory. When the same file is run
# MAGIC locally, `__file__` identifies that directory instead.

# COMMAND ----------

import json
import pprint
import re
import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
CONFIG_PATH = REPO_ROOT / "config.json"
TEMPLATE_ROOT = REPO_ROOT / "templates"

print(f"Repository root : {REPO_ROOT}")
print(f"Configuration   : {CONFIG_PATH}")
print(f"Templates       : {TEMPLATE_ROOT}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load and inspect `config.json`
# MAGIC
# MAGIC The configuration describes one JSON-file source system and the datasets
# MAGIC arriving from it. Only lightweight validation is performed here. Databricks
# MAGIC bundle validation remains responsible for checking the generated resource YAML.

# COMMAND ----------

# Bundle and dataset names become Python names, folder names, and DAB resource keys.
VALID_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def load_config() -> dict:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

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


CONFIG = load_config()
BUNDLE = CONFIG["bundle"]
TARGETS = CONFIG["targets"]

print(f"Loaded bundle '{BUNDLE['name']}' with {len(CONFIG['datasets'])} datasets:")
for dataset in CONFIG["datasets"]:
    schedule = dataset["schedule"]
    print(
        f"  • {dataset['name']:<10} "
        f"source={dataset['source']['path']:<10} "
        f"schedule={schedule['quartz_cron_expression']} ({schedule['timezone_id']})"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Define the small template engine
# MAGIC
# MAGIC Templates contain the fixed, pre-approved shape of the bundle and pipeline
# MAGIC code. The factory replaces uppercase placeholders such as `{{DATASET_NAME}}`.
# MAGIC This keeps the generation logic visible and avoids introducing a templating
# MAGIC framework for four small files.

# COMMAND ----------

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
    """Create parent directories and write one generated file."""
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


template_names = sorted(
    path.name.removesuffix(".tmpl") for path in TEMPLATE_ROOT.glob("*.tmpl")
)
print(f"Template engine ready. Found {len(template_names)} templates:")
for template_name in template_names:
    print(f"  • {template_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Prepare the generated bundle folder
# MAGIC
# MAGIC `generated/<bundle-name>/` contains generated output only. Recreating the
# MAGIC directory on every run prevents files for removed datasets from being left
# MAGIC behind.

# COMMAND ----------

BUNDLE_ROOT = REPO_ROOT / "generated" / BUNDLE["name"]

if BUNDLE_ROOT.exists():
    shutil.rmtree(BUNDLE_ROOT)
    print(f"Removed previous generated bundle: {BUNDLE_ROOT}")

BUNDLE_ROOT.mkdir(parents=True)
print(f"Created clean bundle folder: {BUNDLE_ROOT}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Generate `databricks.yml`
# MAGIC
# MAGIC The root bundle file declares shared variables and the development and
# MAGIC production targets. Environment-specific schemas and schedule states come
# MAGIC directly from `config.json`.

# COMMAND ----------

# `json.dumps` safely quotes strings; JSON strings are also valid YAML strings.
bundle_yaml = render(
    "databricks.yml",
    {
        "BUNDLE_NAME": BUNDLE["name"],
        "CATALOG": json.dumps(BUNDLE["catalog"]),
        "SOURCE_SCHEMA": json.dumps(BUNDLE["source_schema"]),
        "SOURCE_VOLUME": json.dumps(BUNDLE["source_volume"]),
        "DEV_SCHEMA": json.dumps(TARGETS["dev"]["schema"]),
        "DEV_PAUSE_STATUS": TARGETS["dev"]["schedule_pause_status"],
        "PROD_SCHEMA": json.dumps(TARGETS["prod"]["schema"]),
        "PROD_PAUSE_STATUS": TARGETS["prod"]["schedule_pause_status"],
    },
)

write(BUNDLE_ROOT / "databricks.yml", bundle_yaml)
print("✓ Generated databricks.yml")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Generate each dataset pipeline
# MAGIC
# MAGIC For every configured dataset this cell creates three files:
# MAGIC
# MAGIC - A pipeline resource containing one Bronze and one Silver table.
# MAGIC - A Lakeflow Job resource that runs the pipeline on its configured schedule.
# MAGIC - A Python transformation file built from the standard Bronze/Silver template.
# MAGIC
# MAGIC Auto Loader options, Silver selections, and data-quality expectations are
# MAGIC inserted from the dataset's configuration.

# COMMAND ----------

def generate_dataset(dataset: dict) -> list[Path]:
    name = dataset["name"]
    source = dataset["source"]
    silver = dataset["silver"]
    schedule = dataset["schedule"]
    pipeline_key = f"{name}_pipeline"

    pipeline_path = BUNDLE_ROOT / f"resources/{name}.pipeline.yml"
    write(
        pipeline_path,
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
    job_path = BUNDLE_ROOT / f"resources/{name}.job.yml"
    write(
        job_path,
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

    transformation_path = BUNDLE_ROOT / f"src/pipelines/{name}/transformations/{name}.py"
    write(
        transformation_path,
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

    return [pipeline_path, job_path, transformation_path]


for dataset in CONFIG["datasets"]:
    generated_paths = generate_dataset(dataset)
    print(f"✓ Generated {dataset['name']} pipeline")
    for path in generated_paths:
        print(f"    {path.relative_to(BUNDLE_ROOT)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Review the generated bundle
# MAGIC
# MAGIC The final cell lists every generated artifact. These are the files that can
# MAGIC be reviewed, committed, validated, and deployed as a Databricks bundle.

# COMMAND ----------

generated_files = sorted(
    path.relative_to(BUNDLE_ROOT)
    for path in BUNDLE_ROOT.rglob("*")
    if path.is_file()
)

print(f"Bundle generation complete: {BUNDLE_ROOT}")
print(f"Generated {len(generated_files)} files:")
for generated_file in generated_files:
    print(f"  • {generated_file}")
