import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

def transform_customer_to_silver(**kwargs):

    # -------------------------------------
    # 1. Spark Session With Delta
    # -------------------------------------
    builder = (
        SparkSession.builder
        .appName("CustomerSilver")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------
    # 2. Paths
    # -------------------------------------
    bronze_path = "/opt/airflow/data/Bronze/delta/Customer"
    silver_customer_path = "/opt/airflow/data/Silver/delta/Customer"

    if not os.path.exists(bronze_path):
        raise Exception(f"Bronze path not found: {bronze_path}")

    # -------------------------------------
    # 3. Read latest Bronze batch
    # -------------------------------------
    bronze_df = spark.read.format("delta").load(bronze_path)

    latest_df = (
        bronze_df
        .filter(F.col("islatest") == True)
        .orderBy(F.col("creationdate").desc())
        .limit(1)
    )

    # -------------------------------------
    # 4. Customer JSON Schema
    # -------------------------------------
    customer_schema = ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_group", StringType(), True),
            StructField("customer_type", StringType(), True),
            StructField("territory", StringType(), True),
            StructField("customer_primary_address", StringType(), True),
            StructField("sales_team_sales_person", StringType(), True),
            StructField("sales_team_name", StringType(), True),
            StructField("is_internal_customer", DoubleType(), True),
            StructField("email_id", StringType(), True),
            StructField("mobile_no", StringType(), True),
            StructField("creation", StringType(), True),
            StructField("modified", StringType(), True),
        ])
    )

    # -------------------------------------
    # 5. Expand JSON
    # -------------------------------------
    expanded_df = (
        latest_df
        .select(
            F.explode(
                F.from_json(F.col("data"), customer_schema)
            ).alias("c"),
            "batchid",
            "creationdate",
            "md5"
        )
    )

    # -------------------------------------
    # 6. Build Silver Customer Table
    # -------------------------------------
    silver_df = (
        expanded_df
        .select(
            F.col("c.name").alias("customer_id"),
            F.col("c.customer_name").alias("customer"),
            F.col("c.customer_group").alias("customer_group"),
            F.col("c.customer_type").alias("customer_type"),
            F.col("c.territory").alias("territory"),
            F.col("c.customer_primary_address").alias("customer_primary_address"),
            F.col("c.sales_team_sales_person").alias("sales_team_sales_person"),
            F.col("c.sales_team_name").alias("sales_team_name"),
            F.col("c.is_internal_customer").alias("is_internal_customer"),
            F.col("c.email_id").alias("email_id"),
            F.col("c.mobile_no").alias("mobile_no"),
            F.col("c.creation").alias("creation"),
            F.col("c.modified").alias("modified"),
            "batchid",
            "creationdate",
            "md5"
        )
    )

    # -------------------------------------
    # 7. Ensure Delta Table Exists (CRITICAL)
    # -------------------------------------
    silver_path = Path(silver_customer_path)

    if not DeltaTable.isDeltaTable(spark, silver_customer_path):
        (
            silver_df
            .limit(0)
            .write
            .format("delta")
            .mode("overwrite")
            .save(silver_customer_path)
        )

    # -------------------------------------
    # 8. Write to Silver (SAFE OVERWRITE)
    # -------------------------------------
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_customer_path)
    )

    print(f"✅ Silver Customer table written to: {silver_customer_path}")

    # -------------------------------------
    # 9. Sample Output (Debug)
    # -------------------------------------
    silver_df.show(20, truncate=False)
