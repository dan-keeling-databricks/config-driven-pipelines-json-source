# GENERATED FILE. Edit config.yml or templates, then rerun factory.py.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


BRONZE_TABLE = 'bronze_products'
SILVER_TABLE = 'silver_products'
SILVER_READS_FROM = 'bronze_products'
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
    name=BRONZE_TABLE,
    comment='Raw product catalogue records',
)
def ingest_bronze():
    reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", SOURCE_FORMAT)
    for option_name, option_value in SOURCE_OPTIONS.items():
        reader = reader.option(option_name, option_value)

    return (
        reader.load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name=SILVER_TABLE,
    comment='Cleansed product catalogue data',
)
@dp.expect_all({'category_present': 'category IS NOT NULL'})
@dp.expect_all_or_drop({'non_negative_unit_price': 'unit_price IS NOT NULL AND unit_price >= 0',
 'product_id_present': 'product_id IS NOT NULL'})
def cleanse_silver():
    return spark.readStream.table(SILVER_READS_FROM).selectExpr(*SILVER_SELECT)
