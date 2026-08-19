bronze_to_silver.py



from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

# ==========================================================
# BRONZE TO SILVER - DATABRICKS VERSION
# FIXED TIMESTAMP PARSING
# Purpose:
# 1. Read Bronze Delta tables
# 2. Apply data quality checks on orders
# 3. Separate valid and rejected orders
# 4. Clean and deduplicate product master
# 5. Enrich valid orders with product details
# 6. Write curated and rejected outputs to Silver Delta tables
# ==========================================================

# ----------------------------------------------------------
# 1. Generate pipeline run id
# ----------------------------------------------------------
pipeline_run_id = str(uuid.uuid4())

# ----------------------------------------------------------
# 2. Read Bronze tables
# ----------------------------------------------------------
orders_df = spark.table("ecom_temp.bronze.orders_bronze")
products_df = spark.table("ecom_temp.bronze.products_bronze")

# ----------------------------------------------------------
# 3. Helper function
# Convert blank strings into NULL
# ----------------------------------------------------------
def blank_as_null(column_name):
    return F.when(F.trim(F.col(column_name)) == "", None).otherwise(F.col(column_name))

# ----------------------------------------------------------
# 4. Standardize Bronze Orders before validation
# FIX:
# Support both:
# - 2026-03-20 09:15:00
# - 2026-03-21T13:44:00
# ----------------------------------------------------------
orders_prepared_df = (
    orders_df
    .withColumn("order_id", blank_as_null("order_id"))
    .withColumn("customer_id", blank_as_null("customer_id"))
    .withColumn("product_id", blank_as_null("product_id"))
    .withColumn("order_status_std", F.lower(F.trim(F.col("order_status"))))
    .withColumn("quantity_num", F.col("quantity").cast("int"))
    .withColumn("unit_price_num", F.col("unit_price").cast("double"))
    .withColumn(
        "order_timestamp",
        F.coalesce(
            F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd'T'HH:mm:ss"))
        )
    )
)

# ----------------------------------------------------------
# 5. Detect duplicate order_id values
# Keep first occurrence, reject later duplicates
# ----------------------------------------------------------
order_dup_window = Window.partitionBy("order_id").orderBy(
    F.col("load_date").asc(),
    F.col("source_file_name").asc(),
    F.col("ingestion_timestamp").asc()
)

orders_with_dup_flag_df = (
    orders_prepared_df
    .withColumn("dup_rank", F.row_number().over(order_dup_window))
)

# ----------------------------------------------------------
# 6. Apply Data Quality Rules on Orders
# ----------------------------------------------------------
orders_validated_df = (
    orders_with_dup_flag_df
    .withColumn(
        "rejection_reason",
        F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID"))
         .when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID"))
         .when(F.col("product_id").isNull(), F.lit("NULL_PRODUCT_ID"))
         .when(F.col("order_timestamp").isNull(), F.lit("INVALID_ORDER_DATE"))
         .when(F.col("quantity_num").isNull(), F.lit("NULL_OR_INVALID_QUANTITY"))
         .when(F.col("quantity_num") <= 0, F.lit("INVALID_QUANTITY"))
         .when(F.col("unit_price_num").isNull(), F.lit("NULL_OR_INVALID_UNIT_PRICE"))
         .when(F.col("unit_price_num") <= 0, F.lit("INVALID_UNIT_PRICE"))
         .when(~F.col("order_status_std").isin("completed", "pending", "cancelled"), F.lit("INVALID_ORDER_STATUS"))
         .when((F.col("order_id").isNotNull()) & (F.col("dup_rank") > 1), F.lit("DUPLICATE_ORDER_ID"))
         .otherwise(F.lit(None))
    )
)

# ----------------------------------------------------------
# 7. Separate rejected and valid orders
# ----------------------------------------------------------
rejected_orders_df = (
    orders_validated_df
    .filter(F.col("rejection_reason").isNotNull())
    .select(
        F.to_json(
            F.struct(
                F.col("order_id"),
                F.col("customer_id"),
                F.col("product_id"),
                F.col("order_date"),
                F.col("quantity"),
                F.col("unit_price"),
                F.col("order_status")
            )
        ).alias("raw_record"),
        F.col("rejection_reason"),
        F.col("source_file_name"),
        F.col("load_date"),
        F.lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

valid_orders_df = (
    orders_validated_df
    .filter(F.col("rejection_reason").isNull())
)

# ----------------------------------------------------------
# 8. Clean Product Master
# ----------------------------------------------------------
products_prepared_df = (
    products_df
    .withColumn("product_id", blank_as_null("product_id"))
    .withColumn("product_name", blank_as_null("product_name"))
    .withColumn("category", blank_as_null("category"))
    .withColumn("brand", blank_as_null("brand"))
)

products_valid_df = (
    products_prepared_df
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("product_name").isNotNull())
    .filter(F.col("category").isNotNull())
    .filter(F.col("brand").isNotNull())
)

# ----------------------------------------------------------
# 9. Deduplicate Product Master on product_id
# ----------------------------------------------------------
product_dup_window = Window.partitionBy("product_id").orderBy(
    F.col("load_date").asc(),
    F.col("source_file_name").asc(),
    F.col("ingestion_timestamp").asc()
)

products_dedup_df = (
    products_valid_df
    .withColumn("prod_rank", F.row_number().over(product_dup_window))
    .filter(F.col("prod_rank") == 1)
    .select(
        F.col("product_id").alias("p_product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("brand")
    )
)

# ----------------------------------------------------------
# 10. Enrich valid orders with cleaned product master
# ----------------------------------------------------------
orders_curated_df = (
    valid_orders_df.alias("o")
    .join(
        products_dedup_df.alias("p"),
        F.col("o.product_id") == F.col("p.p_product_id"),
        "left"
    )
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.product_id").alias("product_id"),
        F.coalesce(F.col("p.product_name"), F.lit("UNKNOWN")).alias("product_name"),
        F.coalesce(F.col("p.category"), F.lit("UNKNOWN")).alias("category"),
        F.coalesce(F.col("p.brand"), F.lit("UNKNOWN")).alias("brand"),
        F.col("o.order_timestamp").alias("order_timestamp"),
        F.to_date(F.col("o.order_timestamp")).alias("order_date"),
        F.col("o.quantity_num").alias("quantity"),
        F.col("o.unit_price_num").alias("unit_price"),
        (F.col("o.quantity_num") * F.col("o.unit_price_num")).cast("double").alias("order_amount"),
        F.col("o.order_status_std").alias("order_status"),
        F.col("o.load_date").alias("load_date"),
        F.lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

# ----------------------------------------------------------
# 11. Write Silver Curated Orders table
# ----------------------------------------------------------
orders_curated_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecom_temp.silver.orders_curated")

# ----------------------------------------------------------
# 12. Write Silver Rejected Orders table
# ----------------------------------------------------------
rejected_orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecom_temp.silver.rejected_orders")

# ----------------------------------------------------------
# 13. Validation
# ----------------------------------------------------------
print("Pipeline Run ID:", pipeline_run_id)

print("Curated Orders Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM ecom_temp.silver.orders_curated").show()

print("Rejected Orders Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM ecom_temp.silver.rejected_orders").show()

print("Rejection Summary:")
spark.sql("""
SELECT rejection_reason, COUNT(*) AS cnt
FROM ecom_temp.silver.rejected_orders
GROUP BY rejection_reason
ORDER BY cnt DESC
""").show()

print("Curated Orders Preview:")
display(spark.table("ecom_temp.silver.orders_curated"))

print("Rejected Orders Preview:")
display(spark.table("ecom_temp.silver.rejected_orders"))



