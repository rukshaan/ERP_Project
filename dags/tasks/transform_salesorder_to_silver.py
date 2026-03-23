import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

def transform_salesorder_to_silver(**kwargs):
    builder = (
        SparkSession.builder
        .appName("SalesOrderSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_path = "/opt/airflow/data/Bronze/delta/SalesOrder"
    silver_path = "/opt/airflow/data/Silver/delta/SalesOrderItems"

    if not os.path.exists(bronze_path):
        raise Exception(f"Bronze path not found: {bronze_path}")

    bronze_df = spark.read.format("delta").load(bronze_path)
    latest_df = (
        bronze_df.filter(F.col("islatest") == True)
                 .orderBy(F.col("creationdate").desc())
                 .limit(1)
    )

    # --------------------------
    # Schemas
    # --------------------------
    item_schema = ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("item_code", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("qty", DoubleType(), True),
            StructField("delivered_qty", DoubleType(), True),
            StructField("rate", DoubleType(), True),
            StructField("amount", DoubleType(), True),
            StructField("warehouse", StringType(), True),
        ])
    )

    payment_schema = ArrayType(
        StructType([
            StructField("base_payment_amount", DoubleType(), True),
            StructField("due_date", StringType(), True),
            StructField("payment_amount", DoubleType(), True),
            StructField("outstanding", DoubleType(), True),
            StructField("invoice_portion", DoubleType(), True),
        ])
    )

    order_schema = ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("customer", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("delivery_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("company", StringType(), True),
            StructField("grand_total", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("quotation_item", StringType(), True),  # Parent quotation
            StructField("items", item_schema, True),
            StructField("payment_schedule", payment_schema, True),
            StructField("modified", StringType(), True),
            StructField("per_delivered", DoubleType(), True),
            StructField("per_billed", DoubleType(), True),
        ])
    )

    # --------------------------
    # Explode orders (data is array of orders)
    # --------------------------
    expanded_df = latest_df.select(
        F.explode(F.from_json(F.col("data"), order_schema)).alias("so"),
        "md5", "batchid", "creationdate"
    )

    # --------------------------
    # Explode items and payment schedules
    # --------------------------
    exploded_df = expanded_df.withColumn("item", F.explode_outer("so.items")) \
                             .withColumn("payment", F.explode_outer("so.payment_schedule"))

    # --------------------------
    # Final select
    # --------------------------
    silver_df = exploded_df.select(
        F.col("so.name").alias("sales_order_id"),
        F.col("so.customer"),
        F.col("so.transaction_date").alias("order_date"),
        F.col("so.delivery_date"),
        F.col("so.status"),
        F.col("so.company"),
        F.col("so.grand_total"),
        F.col("so.currency"),
        F.col("so.quotation_item"),  # Now preserved correctly
        F.col("so.modified"),
        F.col("so.per_delivered"),
        F.col("so.per_billed"),
        F.col("item.name").alias("item_id"),
        F.col("item.item_code"),
        F.col("item.item_name"),
        F.col("item.qty"),
        F.col("item.delivered_qty"),
        F.col("item.rate"),
        F.col("item.amount"),
        F.col("item.warehouse"),
        F.col("payment.base_payment_amount"),
        F.col("payment.payment_amount"),
        F.col("payment.outstanding"),
        F.col("payment.invoice_portion"),
        (F.col("item.qty") - F.col("item.delivered_qty")).alias("open_qty"),
        ((F.col("item.qty") - F.col("item.delivered_qty")) * F.col("item.rate")).alias("open_amount"),
        F.when((F.col("item.qty") - F.col("item.delivered_qty")) == 0, "Yes").otherwise("No").alias("is_fully_delivered"),
        "batchid",
        "creationdate",
        "md5"
    )

    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    print(f"✅ Silver table written → {silver_path}")
    silver_df.show(50, truncate=False)