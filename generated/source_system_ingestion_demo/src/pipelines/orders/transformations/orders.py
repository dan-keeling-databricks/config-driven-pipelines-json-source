# GENERATED FILE. Edit config.json or templates, then rerun factory.py.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


DATASET_NAME = "orders"
SOURCE_PATH = spark.conf.get("source_path")
SOURCE_FORMAT = spark.conf.get("source_format")
SOURCE_OPTIONS = {'cloudFiles.includeExistingFiles': True,
 'cloudFiles.schemaEvolutionMode': 'addNewColumns',
 'cloudFiles.schemaHints': 'order_id STRING, customer_id STRING, product_id STRING, '
                           'order_timestamp TIMESTAMP, quantity INT, amount DECIMAL(12,2), status '
                           'STRING',
 'rescuedDataColumn': '_rescued_data'}
SILVER_SELECT = ['trim(order_id) AS order_id',
 'trim(customer_id) AS customer_id',
 'trim(product_id) AS product_id',
 'order_timestamp',
 'quantity',
 'amount',
 'upper(trim(status)) AS status',
 '_ingested_at',
 '_source_file',
 '_rescued_data']


@dp.table(
    name="bronze_orders",
    comment=f"Raw incrementally ingested records for {DATASET_NAME}",
)
def bronze_orders():
    reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", SOURCE_FORMAT)
    for option_name, option_value in SOURCE_OPTIONS.items():
        reader = reader.option(option_name, option_value)

    return (
        reader.load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="silver_orders",
    comment='Customer order data',
)
@dp.expect_all({'known_order_status': "status IN ('PENDING', 'SHIPPED', 'COMPLETE', 'CANCELLED')"})
@dp.expect_all_or_drop({'customer_id_present': 'customer_id IS NOT NULL',
 'non_negative_amount': 'amount IS NOT NULL AND amount >= 0',
 'order_id_present': 'order_id IS NOT NULL',
 'positive_quantity': 'quantity IS NOT NULL AND quantity > 0',
 'product_id_present': 'product_id IS NOT NULL'})
def silver_orders():
    return spark.readStream.table("bronze_orders").selectExpr(*SILVER_SELECT)
