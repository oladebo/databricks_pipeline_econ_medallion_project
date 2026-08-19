source_to_bronze.py



from pyspark.sql.functions import current_timestamp, lit, regexp_extract, col
import uuid

# ==========================================================
# RAW TO BRONZE INGESTION - DATABRICKS VERSION
# Purpose:
# 1. Read raw JSON files from Unity Catalog volumes
# 2. Add ingestion metadata
# 3. Extract load_date from folder path
# 4. Write data into Bronze Delta tables
# ==========================================================

# ----------------------------------------------------------
# 1. Define source paths
# ----------------------------------------------------------
orders_path = "/Volumes/ecom_temp/data_layer/source_1/load_date=2026-04-03/"
products_path = "/Volumes/ecom_temp/data_layer/source_2/load_date=2026-04-03/"

# ----------------------------------------------------------
# 2. Generate pipeline run id
# ----------------------------------------------------------
pipeline_run_id = str(uuid.uuid4())

# ----------------------------------------------------------
# 3. Read raw JSON files from volumes
# Note:
# input_file_name() is not supported with Unity Catalog
# so we use _metadata.file_path instead
# ----------------------------------------------------------
orders_df = (
    spark.read
    .format("json")
    .option("multiLine", "true")
    .load(orders_path)
    .select("*", "_metadata")
)

products_df = (
    spark.read
    .format("json")
    .option("multiLine", "true")
    .load(products_path)
    .select("*", "_metadata")
)

# ----------------------------------------------------------
# 4. Add metadata columns
# Metadata added:
# - source_file_name
# - ingestion_timestamp
# - pipeline_run_id
# - load_date
# ----------------------------------------------------------
orders_bronze_df = (
    orders_df
    .withColumn("source_file_name", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
    .withColumn(
        "load_date",
        regexp_extract(col("_metadata.file_path"), r"load_date=(\d{4}-\d{2}-\d{2})", 1)
    )
    .drop("_metadata")
)

products_bronze_df = (
    products_df
    .withColumn("source_file_name", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
    .withColumn(
        "load_date",
        regexp_extract(col("_metadata.file_path"), r"load_date=(\d{4}-\d{2}-\d{2})", 1)
    )
    .drop("_metadata")
)

# ----------------------------------------------------------
# 5. Write Orders Bronze table
# ----------------------------------------------------------
orders_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .saveAsTable("ecom_temp.bronze.orders_bronze")

# ----------------------------------------------------------
# 6. Write Products Bronze table
# ----------------------------------------------------------
products_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .saveAsTable("ecom_temp.bronze.products_bronze")

# ----------------------------------------------------------
# 7. Validation
# ----------------------------------------------------------
print("Pipeline Run ID:", pipeline_run_id)

print("Orders Bronze Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM ecom_temp.bronze.orders_bronze").show()

print("Products Bronze Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM ecom_temp.bronze.products_bronze").show()

print("Orders Bronze Preview:")
display(spark.table("ecom_temp.bronze.orders_bronze"))

print("Products Bronze Preview:")
display(spark.table("ecom_temp.bronze.products_bronze"))







