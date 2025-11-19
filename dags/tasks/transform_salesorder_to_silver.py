import os
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

def transform_salesorder_to_silver(**kwargs):
    print(">>>> Silver task has started <<<<")

    # -------------------------------------
    # 1. Spark Session With Delta
    # -------------------------------------
    builder = (
        SparkSession.builder
        .appName("SalesOrderSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_path = "/opt/airflow/data/Bronze/delta/SalesOrder"
    silver_header_path = "/opt/airflow/data/Silver/delta/SalesOrderHeader"
    silver_items_path = "/opt/airflow/data/Silver/delta/SalesOrderItems"

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
    sales_order_schema = ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("customer", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("delivery_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("company", StringType(), True),
            StructField("grand_total", DoubleType(), True),
            StructField("currency", StringType(), True),

            StructField("items", ArrayType(
                StructType([
                    StructField("item_code", StringType(), True),
                    StructField("item_name", StringType(), True),
                    StructField("qty", DoubleType(), True),
                    StructField("rate", DoubleType(), True),
                    StructField("amount", DoubleType(), True),
                ])
            ), True),
        ])
    )

    # -------------------------------------
    # 4. Expand JSON
    # -------------------------------------
    expanded_df = (
        latest_df
        .select(
            F.explode(F.from_json(F.col("data"), sales_order_schema)).alias("so"),
            "md5", "batchid", "creationdate"
        )
    )

    # -------------------------------------
    # 5. SILVER HEADER TABLE
    # -------------------------------------
    silver_header_df = expanded_df.select(
        F.col("so.name").alias("sales_order_id"),
        F.col("so.customer").alias("customer"),
        F.col("so.transaction_date").alias("order_date"),
        F.col("so.delivery_date").alias("delivery_date"),
        F.col("so.status").alias("status"),
        F.col("so.company").alias("company"),
        F.col("so.grand_total").alias("grand_total"),
        F.col("so.currency").alias("currency"),
        "batchid", "creationdate", "md5"
    )

    (
        silver_header_df
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_header_path)
    )

    print(f"Silver SalesOrderHeader written → {silver_header_path}")

    # -------------------------------------
    # 6. SILVER ITEMS TABLE (one row per item)
    # -------------------------------------
    silver_items_df = (
        expanded_df
        .select(
            F.col("so.name").alias("sales_order_id"),
            F.explode("so.items").alias("item"),
            "batchid", "creationdate", "md5"
        )
        .select(
            "sales_order_id",
            F.col("item.item_code").alias("item_code"),
            F.col("item.item_name").alias("item_name"),
            F.col("item.qty").alias("item_quantity"),
            F.col("item.rate").alias("item_rate"),
            F.col("item.amount").alias("item_amount"),
            "batchid", "creationdate", "md5"
        )
    )

    (
        silver_items_df
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_items_path)
    )

    print(f"Silver SalesOrderItems written → {silver_items_path}")

    # Show sample
    silver_items_df.show(20, truncate=False)
