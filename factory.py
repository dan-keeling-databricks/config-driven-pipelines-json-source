# Databricks notebook source
# MAGIC %md
# MAGIC # Config-driven pipeline factory
# MAGIC
# MAGIC This notebook turns `config.yml` into a complete Databricks bundle.
# MAGIC
# MAGIC It demonstrates a deliberately simple factory pattern:
# MAGIC
# MAGIC 1. Read the source-system and workflow configuration.
# MAGIC 2. Fill a small set of pre-built templates.
# MAGIC 3. Generate one explicitly named Databricks Pipeline and Databricks Job
# MAGIC    per workflow.
# MAGIC 4. Write the deployable bundle under `generated/`.
# MAGIC
# MAGIC The notebook prints progress as each part of the bundle is created.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Locate the Git folder
# MAGIC
# MAGIC The notebook is stored at the root of the Databricks Git folder, alongside
# MAGIC `config.yml` and the `templates/` directory. When the same file is run
# MAGIC locally, `__file__` identifies that directory instead.

# COMMAND ----------

import json
import pprint
import re
import shutil
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
CONFIG_PATH = REPO_ROOT / "config.yml"
TEMPLATE_ROOT = REPO_ROOT / "templates"

print(f"Repository root : {REPO_ROOT}")
print(f"Configuration   : {CONFIG_PATH}")
print(f"Templates       : {TEMPLATE_ROOT}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load and inspect `config.yml`
# MAGIC
# MAGIC Each workflow contains a Databricks Pipeline and the Databricks Job that
# MAGIC schedules it. YAML is loaded directly into normal Python dictionaries and
# MAGIC lists, which are then validated before any files are generated.

# COMMAND ----------

# Bundle, resource, and table names become folder names or DAB identifiers.
VALID_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def load_config() -> dict:
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))

    required_sections = {"bundle", "source_system", "targets", "workflows"}
    missing_sections = required_sections - config.keys()
    if missing_sections:
        raise ValueError(f"Missing config sections: {sorted(missing_sections)}")
    if not config["workflows"]:
        raise ValueError("At least one workflow is required")

    workflow_names = [workflow.get("name", "") for workflow in config["workflows"]]
    pipeline_keys = [
        workflow.get("pipeline", {}).get("resource_key", "")
        for workflow in config["workflows"]
    ]
    job_keys = [
        workflow.get("job", {}).get("resource_key", "")
        for workflow in config["workflows"]
    ]
    names = [
        config["bundle"].get("name", ""),
        *workflow_names,
        *pipeline_keys,
        *job_keys,
    ]
    for workflow in config["workflows"]:
        tables = workflow.get("pipeline", {}).get("tables", {})
        names.extend(
            tables.get(layer, {}).get("name", "") for layer in ("bronze", "silver")
        )

    invalid_names = [name for name in names if not VALID_NAME.fullmatch(name)]
    if invalid_names:
        raise ValueError(f"Invalid bundle, resource, or table names: {invalid_names}")

    if len(workflow_names) != len(set(workflow_names)):
        raise ValueError("Workflow names must be unique")
    if len(pipeline_keys) != len(set(pipeline_keys)):
        raise ValueError("Pipeline resource_key values must be unique")
    if len(job_keys) != len(set(job_keys)):
        raise ValueError("Job resource_key values must be unique")

    for workflow in config["workflows"]:
        pipeline = workflow["pipeline"]
        bronze = pipeline["tables"]["bronze"]
        silver = pipeline["tables"]["silver"]
        table_types = {bronze.get("type"), silver.get("type")}
        if table_types != {"streaming_table"}:
            raise ValueError("This demo template supports streaming_table entries only")
        if silver.get("reads_from") != bronze["name"]:
            raise ValueError(
                f"{workflow['name']}: silver.reads_from must equal bronze.name"
            )

    return config


CONFIG = load_config()
BUNDLE = CONFIG["bundle"]
SOURCE_SYSTEM = CONFIG["source_system"]
TARGETS = CONFIG["targets"]
WORKFLOWS = CONFIG["workflows"]

print(
    "Source system : "
    f"{SOURCE_SYSTEM['type']} at "
    f"{SOURCE_SYSTEM['catalog']}.{SOURCE_SYSTEM['schema']}.{SOURCE_SYSTEM['volume']}"
)
print(f"Loaded bundle '{BUNDLE['name']}' with {len(WORKFLOWS)} workflows:")
for workflow in WORKFLOWS:
    pipeline = workflow["pipeline"]
    job = workflow["job"]
    bronze = pipeline["tables"]["bronze"]
    silver = pipeline["tables"]["silver"]
    schedule = job["schedule"]
    print(f"  • {workflow['name']}")
    print(
        f"      Pipeline {pipeline['resource_key']}: "
        f"{bronze['name']} → {silver['name']} "
        f"from {bronze['source']['path']}"
    )
    print(
        f"      Job {job['resource_key']}: "
        f"{schedule['quartz_cron_expression']} ({schedule['timezone_id']})"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Define the small template engine
# MAGIC
# MAGIC Templates contain the fixed, pre-approved shape of the bundle and pipeline
# MAGIC code. The factory replaces named placeholders such as
# MAGIC `{{PIPELINE_RESOURCE_KEY}}`.
# MAGIC This keeps the generation logic visible and avoids introducing a templating
# MAGIC framework for four small files.

# COMMAND ----------

PLACEHOLDER = re.compile(r"\{\{([A-Z][A-Z0-9_]*)\}\}")


def populate_template(
    template_name: str,
    placeholder_values: dict[str, str],
) -> str:
    """Load a template and replace its `{{PLACEHOLDER}}` values."""
    template_path = TEMPLATE_ROOT / f"{template_name}.tmpl"
    text = template_path.read_text(encoding="utf-8")

    for name, value in placeholder_values.items():
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
# MAGIC directory on every run prevents files for removed workflows from being left
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
# MAGIC production targets. Source variables always point to the landing volume;
# MAGIC target variables select where each environment publishes its tables.

# COMMAND ----------

# `json.dumps` safely quotes strings; JSON strings are also valid YAML strings.
bundle_yaml = populate_template(
    template_name="databricks.yml",
    placeholder_values={
        "BUNDLE_NAME": BUNDLE["name"],
        "SOURCE_CATALOG": json.dumps(SOURCE_SYSTEM["catalog"]),
        "SOURCE_SCHEMA": json.dumps(SOURCE_SYSTEM["schema"]),
        "SOURCE_VOLUME": json.dumps(SOURCE_SYSTEM["volume"]),
        "SOURCE_FORMAT": json.dumps(SOURCE_SYSTEM["format"]),
        "DEV_CATALOG": json.dumps(TARGETS["dev"]["catalog"]),
        "DEV_SCHEMA": json.dumps(TARGETS["dev"]["schema"]),
        "DEV_PAUSE_STATUS": TARGETS["dev"]["schedule_pause_status"],
        "PROD_CATALOG": json.dumps(TARGETS["prod"]["catalog"]),
        "PROD_SCHEMA": json.dumps(TARGETS["prod"]["schema"]),
        "PROD_PAUSE_STATUS": TARGETS["prod"]["schedule_pause_status"],
    },
)

write(BUNDLE_ROOT / "databricks.yml", bundle_yaml)
print("✓ Generated databricks.yml")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Generate each configured workflow
# MAGIC
# MAGIC For every workflow this cell creates three files:
# MAGIC
# MAGIC - A pipeline resource containing one Bronze and one Silver table.
# MAGIC - A Lakeflow Job resource that runs the pipeline on its configured schedule.
# MAGIC - A Python transformation file built from the standard Bronze/Silver template.
# MAGIC
# MAGIC Auto Loader options, Silver selections, and data-quality expectations are
# MAGIC inserted from the configuration. Table names are copied exactly as written;
# MAGIC the factory does not derive them from the pipeline name.

# COMMAND ----------

def generate_workflow(workflow: dict) -> list[Path]:
    pipeline = workflow["pipeline"]
    job = workflow["job"]
    pipeline_key = pipeline["resource_key"]
    job_key = job["resource_key"]
    bronze = pipeline["tables"]["bronze"]
    silver = pipeline["tables"]["silver"]
    source = bronze["source"]
    schedule = job["schedule"]

    pipeline_path = BUNDLE_ROOT / f"resources/{pipeline_key}.pipeline.yml"
    write(
        pipeline_path,
        populate_template(
            template_name="pipeline.yml",
            placeholder_values={
                "PIPELINE_RESOURCE_KEY": pipeline_key,
                "PIPELINE_DISPLAY_NAME": json.dumps(pipeline["display_name"]),
                "SOURCE_SUBPATH": source["path"].strip("/"),
            },
        ),
    )

    # Pipelines do not contain schedules, so a Lakeflow Job triggers each pipeline.
    job_path = BUNDLE_ROOT / f"resources/{job_key}.job.yml"
    write(
        job_path,
        populate_template(
            template_name="job.yml",
            placeholder_values={
                "JOB_RESOURCE_KEY": job_key,
                "PIPELINE_RESOURCE_KEY": pipeline_key,
                "JOB_DISPLAY_NAME": json.dumps(job["display_name"]),
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

    transformation_path = (
        BUNDLE_ROOT / f"src/pipelines/{pipeline_key}/transformations/pipeline.py"
    )
    write(
        transformation_path,
        populate_template(
            template_name="dataset.py",
            placeholder_values={
                "BRONZE_TABLE_REPR": repr(bronze["name"]),
                "BRONZE_DESCRIPTION_REPR": repr(bronze["description"]),
                "SILVER_TABLE_REPR": repr(silver["name"]),
                "SILVER_DESCRIPTION_REPR": repr(silver["description"]),
                "SILVER_READS_FROM_REPR": repr(silver["reads_from"]),
                "SOURCE_OPTIONS_REPR": python_literal(source_options),
                "SILVER_SELECT_REPR": python_literal(silver["select"]),
                "EXPECTATION_DECORATORS": expectations_as_decorators(
                    silver.get("expectations", {})
                ),
            },
        ),
    )

    return [pipeline_path, job_path, transformation_path]


for workflow in WORKFLOWS:
    generated_paths = generate_workflow(workflow)
    pipeline = workflow["pipeline"]
    job = workflow["job"]
    bronze = pipeline["tables"]["bronze"]["name"]
    silver = pipeline["tables"]["silver"]["name"]
    print(f"✓ Generated workflow '{workflow['name']}'")
    print(f"    Pipeline: {pipeline['resource_key']} ({bronze} → {silver})")
    print(f"    Job:      {job['resource_key']}")
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
