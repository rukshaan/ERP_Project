
import os
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
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
        .appName("SalesOrderSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_path = "/opt/airflow/data/Bronze/delta/Customer"
    
    silver_customer_path = "/opt/airflow/data/Silver/delta/Customer"

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
    sales_customer_schema = ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_group", StringType(), True),
            StructField("customer_type", StringType(), True),
            StructField("territory", StringType(), True),
            StructField("customer_primary_address", StringType(), True),
            StructField("sales_team_sales_person",StringType(), True),
            StructField("sales_team_name", StringType(), True),

             # Required ERPNext additional fields
            StructField("is_internal_customer", DoubleType(), True),
            StructField("email_id", StringType(), True),
            StructField("mobile_no", StringType(), True),
            StructField("creation", StringType(), True),
            StructField("modified", StringType(), True),
            

            
        ])
    )

    # -------------------------------------
    # 4. Expand JSON
    # -------------------------------------
    expanded_df = (
        latest_df
        .select(
            F.explode(F.from_json(F.col("data"), sales_customer_schema)).alias("so"),
            "md5", "batchid", "creationdate"
        )
    )

    

    # -------------------------------------
    # 6. SILVER ITEMS TABLE (one row per item)
    # -------------------------------------
    silver_items_df = (
        expanded_df
        .select(
            F.col("so.name").alias("customer_id"),
            F.col("so.customer_name").alias("customer"),
            F.col("so.customer_group").alias("customer_group"),
            F.col("so.customer_type").alias("customer_type"),
            F.col("so.territory").alias("territory"),
            F.col("so.customer_primary_address").alias("customer_primary_address"),
            F.col("so.sales_team_sales_person").alias("sales_team_sales_person"),
            F.col("so.sales_team_name").alias("sales_team_name"),
            F.col("so.is_internal_customer").alias("is_internal_customer"),
            F.col("so.email_id").alias("email_id"),
            F.col("so.mobile_no").alias("mobile_no"),
            F.col("so.creation").alias("creation"),
            F.col("so.modified").alias("modified"),
            "batchid", "creationdate", "md5"
        )
        .select(
            "customer_id",
            "customer",
            "customer_group",
            "customer_type",
            "territory",
            "customer_primary_address",
            "sales_team_sales_person",
            "sales_team_name",
            "is_internal_customer",
            "email_id",
            "mobile_no",
            "creation",
            "modified",
            "batchid",
            "creationdate",
            "md5"
        )
    )

    (
        silver_items_df
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_customer_path)
    )

    print(f"Silver SalesOrderItems written → {silver_customer_path}")

    # # Show sample
    silver_items_df.show(20, truncate=False)
