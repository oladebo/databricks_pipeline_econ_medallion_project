



from pyspark.sql import functions as F
import uuid

# ==========================================================
# SILVER TO GOLD - DATABRICKS VERSION
# Purpose:
# 1. Read Silver curated orders
# 2. Apply safety filters
# 3. Build daily product sales gold table
# 4. Build category sales gold table
# 5. Write results into Gold Delta tables
# ==========================================================

# ----------------------------------------------------------
# 1. Generate pipeline run id
# ----------------------------------------------------------
pipeline_run_id = str(uuid.uuid4())

# ----------------------------------------------------------
# 2. Read Silver curated orders
# ----------------------------------------------------------
orders_curated_df = spark.table("elab_medallion_pipeline.silver.orders_curated")

# ----------------------------------------------------------
# 3. Apply safety filters before Gold aggregation
# ----------------------------------------------------------
gold_base_df = (
    orders_curated_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("order_date").isNotNull())
    .filter(F.col("quantity").isNotNull())
    .filter(F.col("unit_price").isNotNull())
    .filter(F.col("order_amount").isNotNull())
)

# ----------------------------------------------------------
# 4. Build Gold Table - Daily Product Sales
# Aggregation level:
# one row per order_date + product_id + product_name + category
# ----------------------------------------------------------
daily_product_sales_df = (
    gold_base_df
    .groupBy(
        F.col("order_date"),
        F.col("product_id"),
        F.col("product_name"),
        F.col("category")
    )
    .agg(
        F.countDistinct("order_id").cast("int").alias("total_orders"),
        F.sum("quantity").cast("int").alias("total_quantity"),
        F.round(F.sum("order_amount"), 2).cast("double").alias("total_sales")
    )
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
)

# ----------------------------------------------------------
# 5. Build Gold Table - Category Sales
# Aggregation level:
# one row per order_date + category
# ----------------------------------------------------------
category_sales_df = (
    gold_base_df
    .groupBy(
        F.col("order_date"),
        F.col("category")
    )
    .agg(
        F.countDistinct("order_id").cast("int").alias("total_orders"),
        F.sum("quantity").cast("int").alias("total_quantity"),
        F.round(F.sum("order_amount"), 2).cast("double").alias("total_sales")
    )
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
)

# ----------------------------------------------------------
# 6. Write Gold Daily Product Sales table
# ----------------------------------------------------------
daily_product_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("elab_medallion_pipeline.gold.daily_product_sales")

# ----------------------------------------------------------
# 7. Write Gold Category Sales table
# ----------------------------------------------------------
category_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("elab_medallion_pipeline.gold.category_sales")

# ----------------------------------------------------------
# 8. Validation
# ----------------------------------------------------------
print("Pipeline Run ID:", pipeline_run_id)

print("Daily Product Sales Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM elab_medallion_pipeline.gold.daily_product_sales").show()

print("Category Sales Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM elab_medallion_pipeline.gold.category_sales").show()

print("Daily Product Sales Preview:")
display(spark.table("elab_medallion_pipeline.gold.daily_product_sales"))

print("Category Sales Preview:")
display(spark.table("elab_medallion_pipeline.gold.category_sales"))








