# Config-driven Databricks ingestion DAB

This project demonstrates a small Databricks notebook factory that turns one
readable `config.yml` into a complete Declarative Automation Bundle (DAB).

## Work in the Databricks UI

Create a Databricks Git folder using this repository URL:

```text
https://github.com/dan-keeling-databricks/config-driven-pipelines-json-source
```

Then use this workflow in the Databricks UI:

1. Run `upload_mock_data.py` once to create and populate the landing volume.
2. Edit `config.yml` in the Git folder.
3. Open `factory.py`; Databricks recognizes it as a source-format Python notebook.
4. Attach compute and run the notebook.
5. Review the regenerated files under `generated/source_system_ingestion_demo/`.
6. Commit and push the configuration and generated files from the Git folder UI.

Because `factory.py` is stored at the repository root, it discovers the Git
folder automatically when run in Databricks.

## What the configuration describes

The top-level sections have distinct responsibilities:

- `bundle` names the generated DAB.
- `source_system` identifies the shared JSON landing volume.
- `targets` identifies each environment's output catalog, schema, and schedule state.
- `workflows` lists the ingestion workflows to generate.

Each workflow explicitly separates the Databricks Pipeline that creates the
tables from the Databricks Job that schedules that pipeline:

```yaml
workflows:
  - name: customers_ingestion
    pipeline:
      resource_key: customers_pipeline
      display_name: Customers ingestion
      tables:
        bronze:
          name: bronze_customers
          type: streaming_table
          description: Raw customer records
          source:
            path: customers
            schema_hints: customer_id STRING, ...
            options: {}
        silver:
          name: silver_customers
          type: streaming_table
          description: Cleansed customer master data
          reads_from: bronze_customers
          select:
            - trim(customer_id) AS customer_id
          expectations:
            warn: {}
            drop: {}
            fail: {}
    job:
      resource_key: customers_pipeline_schedule
      display_name: Customers ingestion schedule
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
        timezone_id: Europe/London
```

The factory copies the Pipeline key, Job key, table names, dependency, and
schedule directly from the workflow. It does not infer those resource names.
`yaml.safe_load()` parses the YAML directly into ordinary Python dictionaries
and lists, so no intermediate JSON conversion is needed.

The initial configuration generates three workflows:

| Workflow | Pipeline tables | Scheduled Job |
|---|---|---|
| `customers_ingestion` | `bronze_customers` → `silver_customers` | Hourly |
| `orders_ingestion` | `bronze_orders` → `silver_orders` | Every four hours at :15 |
| `products_ingestion` | `bronze_products` → `silver_products` | Daily at 02:30 Europe/London |

## Generate the bundle

Open `factory.py` as a Databricks notebook inside a Git folder and run all
cells. To run the same file locally, first install its one dependency:

```bash
python3 -m pip install -r requirements.txt
python3 factory.py
```

The output is recreated under:

```text
generated/source_system_ingestion_demo/
├── databricks.yml
├── resources/
│   ├── customers_pipeline.pipeline.yml
│   ├── customers_pipeline_schedule.job.yml
│   ├── orders_pipeline.pipeline.yml
│   ├── orders_pipeline_schedule.job.yml
│   ├── products_pipeline.pipeline.yml
│   └── products_pipeline_schedule.job.yml
└── src/pipelines/<pipeline_resource_key>/transformations/pipeline.py
```

Generated files should not be edited directly. Change `config.yml` or a file
under `templates/`, then rerun the notebook. The generated bundle folder is
recreated on every run so removed workflows cannot leave stale resources behind.

## Source data layout

This repository represents one source system: JSON files arriving in this Unity
Catalog managed volume:

```text
/Volumes/classic_stable_dankeeling_catalog/config-driven-pipelines-demo/landing/
```

Its landing folders are:

```text
.../landing/customers/
.../landing/orders/
.../landing/products/
```

The checked-in `mock_data/` directory contains newline-delimited JSON batches
for these folders. Run `upload_mock_data.py` in Databricks to create the source
schema, development target schema, and volume and copy the files into place.
The notebook requires `USE CATALOG`, `CREATE SCHEMA`, and `CREATE VOLUME`
privileges as applicable.

The `dev` bundle target reads that source volume and publishes tables to
`classic_stable_dankeeling_catalog.config-driven-pipelines-demo-dev`. The `prod`
target publishes to `classic_stable_dankeeling_catalog.config-driven-pipelines-demo`.
Development schedules are generated as paused; production schedules are active.

## Validate and deploy

Choose an authenticated Databricks CLI profile explicitly, then run from the
generated bundle directory:

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
```

Deploying the bundle creates the pipeline and job resources. The separate
`upload_mock_data.py` notebook creates the source schema and managed volume and
uploads the mock source files.
