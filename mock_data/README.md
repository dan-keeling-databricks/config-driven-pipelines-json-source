# Mock JSON source data

These files model three feeds from one JSON-file source system. Each file is
newline-delimited JSON so Auto Loader can ingest every line as a record.

The batches tell a simple retail story: a promotion increases order activity,
while a handful of malformed source records demonstrate the generated silver
expectations. Valid orders use customer and product identifiers from the master
files; the intentionally invalid rows contain missing keys, a negative amount,
an invalid quantity, or an unknown status.

Run the repository's `upload_mock_data.py` notebook to copy the dataset folders
to:

```text
/Volumes/classic_stable_dankeeling_catalog/config-driven-pipelines-demo/landing/
```
