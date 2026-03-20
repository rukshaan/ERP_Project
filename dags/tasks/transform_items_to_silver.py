import os
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

def transform_items_to_silver(**kwargs):

    # -------------------------------------
    # 1. Spark Session With Delta
    # -------------------------------------
    builder = (
        SparkSession.builder
        .appName("ItemsSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_path = "/opt/airflow/data/Bronze/delta/Item"
    silver_items_path = "/opt/airflow/data/Silver/delta/Item"

    if not os.path.exists(bronze_path):
        raise Exception(f"Bronze path not found: {bronze_path}")

    # -------------------------------------
    # 2. Read latest bronze batch
    # -------------------------------------
    bronze_df = spark.read.format("delta").load(bronze_path)

    latest_df = (
        bronze_df
        .filter(F.col("islatest") == True)
        .orderBy(F.col("creationdate").desc())
        .limit(1)
    )

    # -------------------------------------
    # 3. Schema definition
    # -------------------------------------
    items_schema = ArrayType(
        StructType([
            StructField("item_code", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("standard_rate", DoubleType(), True),
            StructField("asset_category", StringType(), True),
            StructField("country_of_origin", StringType(), True),
            StructField("customer_code", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("warranty_period", StringType(), True),
            StructField("weight_uom", StringType(), True),
            StructField("weight_per_unit", DoubleType(), True),
            StructField("valid_from", DoubleType(), True),
            StructField("valid_to", StringType(), True),
            StructField("is_current", DoubleType(), True),
        ])
    )

    # -------------------------------------
    # 4. Expand JSON
    # -------------------------------------
    expanded_df = (
        latest_df
        .select(
            F.explode(F.from_json(F.col("data"), items_schema)).alias("item"),
            "md5", "batchid", "creationdate"
        )
    )

    # -------------------------------------
    # 5. SILVER ITEMS TABLE
    # -------------------------------------
    silver_items_df = (
        expanded_df
        .select(
            F.col("item.item_code").alias("item_code"),
            F.col("item.item_name").alias("item_name"),
            F.col("item.standard_rate").alias("standard_rate"),
            F.col("item.asset_category").alias("asset_category"),
            F.col("item.country_of_origin").alias("country_of_origin"),
            F.col("item.customer_code").alias("customer_code"),
            F.col("item.owner").alias("owner"),
            F.col("item.warranty_period").alias("warranty_period"),
            F.col("item.weight_uom").alias("weight_uom"),
            F.col("item.weight_per_unit").alias("weight_per_unit"),
            F.col("item.valid_from").alias("valid_from"),
            F.col("item.valid_to").alias("valid_to"),
            F.col("item.is_current").alias("is_current"),
            "batchid",
            "creationdate",
            "md5"
        )
    )

    # -------------------------------------
    # 6. Write to Silver with Concurrency Handling
    # -------------------------------------
    try:
        (
            silver_items_df
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("replaceWhere", "1=1")  # Safe overwrite
            .save(silver_items_path)
        )
        print(f"✅ Silver Items table written successfully")

    except Exception as e:
        error_msg = str(e)
        if "DELTA_PROTOCOL_CHANGED" in error_msg or "concurrent" in error_msg.lower():
            print(f"⚠️  Concurrent write detected, retrying...")
            import time
            time.sleep(2)

            (
                silver_items_df
                .write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(silver_items_path)
            )
            print(f"✅ Silver Items table written on retry")
        else:
            raise

    print(f"Silver Items table written → {silver_items_path}")

    # Show sample
    silver_items_df.show(20, truncate=False)
