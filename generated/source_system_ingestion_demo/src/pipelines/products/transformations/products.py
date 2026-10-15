# GENERATED FILE. Edit config.json or templates, then rerun factory.py.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


DATASET_NAME = "products"
SOURCE_PATH = spark.conf.get("source_path")
SOURCE_FORMAT = spark.conf.get("source_format")
SOURCE_OPTIONS = {'cloudFiles.includeExistingFiles': True,
 'cloudFiles.schemaEvolutionMode': 'addNewColumns',
 'cloudFiles.schemaHints': 'product_id STRING, product_name STRING, category STRING, unit_price '
                           'DECIMAL(12,2), active BOOLEAN',
 'rescuedDataColumn': '_rescued_data'}
SILVER_SELECT = ['trim(product_id) AS product_id',
 'trim(product_name) AS product_name',
 'upper(trim(category)) AS category',
 'unit_price',
 'coalesce(active, true) AS active',
 '_ingested_at',
 '_source_file',
 '_rescued_data']


@dp.table(
    name="bronze_products",
    comment=f"Raw incrementally ingested records for {DATASET_NAME}",
)
def bronze_products():
    reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", SOURCE_FORMAT)
    for option_name, option_value in SOURCE_OPTIONS.items():
        reader = reader.option(option_name, option_value)

    return (
        reader.load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="silver_products",
    comment='Product catalogue data',
)
@dp.expect_all({'category_present': 'category IS NOT NULL'})
@dp.expect_all_or_drop({'non_negative_unit_price': 'unit_price IS NOT NULL AND unit_price >= 0',
 'product_id_present': 'product_id IS NOT NULL'})
def silver_products():
    return spark.readStream.table("bronze_products").selectExpr(*SILVER_SELECT)
