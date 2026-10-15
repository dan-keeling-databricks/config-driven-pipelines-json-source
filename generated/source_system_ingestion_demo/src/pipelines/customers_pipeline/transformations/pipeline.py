# GENERATED FILE. Edit config.yml or templates, then rerun factory.py.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


BRONZE_TABLE = 'bronze_customers'
SILVER_TABLE = 'silver_customers'
SILVER_READS_FROM = 'bronze_customers'
SOURCE_PATH = spark.conf.get("source_path")
SOURCE_FORMAT = spark.conf.get("source_format")
SOURCE_OPTIONS = {'cloudFiles.includeExistingFiles': True,
 'cloudFiles.schemaEvolutionMode': 'addNewColumns',
 'cloudFiles.schemaHints': 'customer_id STRING, first_name STRING, last_name STRING, email STRING, '
                           'created_at TIMESTAMP',
 'rescuedDataColumn': '_rescued_data'}
SILVER_SELECT = ['trim(customer_id) AS customer_id',
 'initcap(trim(first_name)) AS first_name',
 'initcap(trim(last_name)) AS last_name',
 'lower(trim(email)) AS email',
 'created_at',
 '_ingested_at',
 '_source_file',
 '_rescued_data']


@dp.table(
    name=BRONZE_TABLE,
    comment='Raw customer records',
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
    comment='Cleansed customer master data',
)
@dp.expect_all({'valid_email_shape': "email IS NULL OR email LIKE '%@%'"})
@dp.expect_all_or_drop({'customer_id_present': 'customer_id IS NOT NULL'})
def cleanse_silver():
    return spark.readStream.table(SILVER_READS_FROM).selectExpr(*SILVER_SELECT)
