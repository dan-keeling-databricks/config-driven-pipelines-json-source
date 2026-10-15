# Config-driven Databricks ingestion DAB

This project demonstrates a small Databricks notebook factory that turns one
`config.json` into a complete Declarative Automation Bundle (DAB).

## Work in the Databricks UI

Create a Databricks Git folder using this repository URL:

```text
https://github.com/dan-keeling-databricks/config-driven-pipelines-json-source
```

Then use this workflow in the Databricks UI:

1. Run `upload_mock_data.py` once to create and populate the landing volume.
2. Edit `config.json` in the Git folder.
3. Open `factory.py`; Databricks recognizes it as a source-format Python notebook.
4. Attach compute and run the notebook.
5. Review the regenerated files under `generated/source_system_ingestion_demo/`.
6. Commit and push the configuration and generated files from the Git folder UI.

The notebook normally discovers the Git-folder root automatically. If it does
not, set its `repo_root` widget to the absolute `/Workspace/...` path of the
cloned Git folder before running it.

The initial configuration generates three independent Lakeflow Spark
Declarative Pipelines:

| Dataset | Tables | Schedule |
|---|---|---|
| `customers` | `bronze_customers` → `silver_customers` | Hourly |
| `orders` | `bronze_orders` → `silver_orders` | Every four hours at :15 |
| `products` | `bronze_products` → `silver_products` | Daily at 02:30 Europe/London |

Each pipeline has a separate Lakeflow Job resource because schedules are Job
resources, not pipeline properties.

## Generate the bundle

Open `factory.py` as a Databricks notebook inside a Git folder and run it. The
optional `repo_root` widget can point at the repository root if the notebook's
working directory differs. The same file can also be run locally:

```bash
python3 factory.py
```

The output is recreated under:

```text
generated/source_system_ingestion_demo/
├── databricks.yml
├── generated-manifest.json
├── resources/
│   ├── customers.pipeline.yml
│   ├── customers.job.yml
│   ├── orders.pipeline.yml
│   ├── orders.job.yml
│   ├── products.pipeline.yml
│   └── products.job.yml
└── src/pipelines/<dataset>/transformations/<dataset>.py
```

## Source data layout

This repository represents one source system: JSON files arriving in this Unity
Catalog managed volume:

```text
/Volumes/classic_stable_dankeeling_catalog/config-driven-pipelines-demo/landing/
```

Its dataset folders are:

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

Development schedules are generated as paused. Production schedules are
generated as unpaused.

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
