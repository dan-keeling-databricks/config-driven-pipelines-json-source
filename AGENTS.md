# Declarative Automation Bundles project

This project generates a Databricks Declarative Automation Bundle from
`config.json` using the Databricks notebook `factory.py`.

Generated files under `generated/` must not be edited directly. Change the
configuration or templates and rerun the factory.

Before changing bundle, pipeline, or job code, read the `databricks-core`,
`databricks-dabs`, `databricks-pipelines`, and `databricks-jobs` skills.

Always use an explicit Databricks CLI profile. Validate generated bundles with
`databricks bundle validate --strict --target <target> --profile <profile>`.
