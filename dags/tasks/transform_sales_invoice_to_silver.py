import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.types import *

def transform_sales_invoice_to_silver(**kwargs):

    # -------------------------------------
    # 1. Spark Session
    # -------------------------------------
    builder = (
        SparkSession.builder
        .appName("SalesInvoiceSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.debug.maxToStringFields", "100")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_path = "/opt/airflow/data/Bronze/delta/SalesInvoice"
    silver_path = "/opt/airflow/data/Silver/delta/SalesInvoice"

    # -------------------------------------
    # 2. Validate Bronze
    # -------------------------------------
    if not os.path.exists(bronze_path):
        print(f"⚠️ Bronze path not found: {bronze_path}")
        return

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        print(f"⚠️ Not a Delta table: {bronze_path}")
        return

    bronze_df = spark.read.format("delta").load(bronze_path)

    if bronze_df.rdd.isEmpty():
        print("⚠️ Bronze table is empty")
        return

    # -------------------------------------
    # 3. Get Latest Batch (FIXED)
    # -------------------------------------
    latest_batch_id = (
        bronze_df
        .filter(F.col("islatest") == True)
        .agg(F.max("batchid").alias("max_batch"))
        .collect()[0]["max_batch"]
    )

    latest_df = bronze_df.filter(F.col("batchid") == latest_batch_id)

    print(f"✅ Processing batch: {latest_batch_id}")
    print(f"Rows in batch: {latest_df.count()}")

    # -------------------------------------
    # 4. Define Schema
    # -------------------------------------
    header_schema = StructType([
        StructField("name", StringType()),
        StructField("customer", StringType()),
        StructField("customer_name", StringType()),
        StructField("customer_group", StringType()),
        StructField("territory", StringType()),
        StructField("company", StringType()),
        StructField("posting_date", StringType()),
        StructField("due_date", StringType()),
        StructField("is_pos", IntegerType()),
        StructField("is_return", IntegerType()),
        StructField("currency", StringType()),
        StructField("conversion_rate", DoubleType()),
        StructField("total_qty", DoubleType()),
        StructField("net_total", DoubleType()),
        StructField("grand_total", DoubleType()),
        StructField("rounded_total", DoubleType()),
        StructField("outstanding_amount", DoubleType()),
        StructField("paid_amount", DoubleType()),
        StructField("apply_discount_on", StringType()),
        StructField("discount_amount", DoubleType()),
        StructField("additional_discount_percentage", DoubleType()),
        StructField("customer_address", StringType()),
        StructField("shipping_address", StringType()),
        StructField("status", StringType()),
        StructField("docstatus", IntegerType()),
        StructField("is_internal_customer", IntegerType()),
        StructField("is_discounted", IntegerType()),
        StructField("remarks", StringType()),

        # Nested arrays (KEEP THEM)
        StructField("items", ArrayType(StringType())),
        StructField("payments", ArrayType(StringType())),
        StructField("payment_schedule", ArrayType(StringType())),
        StructField("sales_team", ArrayType(StringType())),
        StructField("taxes", ArrayType(StringType()))
    ])

    # -------------------------------------
    # 5. Parse JSON SAFELY
    # -------------------------------------
    parsed_df = latest_df.withColumn(
        "parsed",
        F.from_json(F.col("data"), ArrayType(header_schema))
    )

    # Debug JSON issues
    parsed_df.select("data", "parsed").show(5, truncate=False)

    # -------------------------------------
    # 6. Explode ONLY top-level array
    # -------------------------------------
    df = parsed_df.select(
        F.explode_outer("parsed").alias("s"),
        "batchid",
        "creationdate",
        "md5"
    )

    print(f"Rows after explode: {df.count()}")

    # -------------------------------------
    # 7. Flatten (NO nested explode)
    # -------------------------------------
    silver_df = df.select(
        F.trim(F.col("s.name")).alias("sales_invoice_id"),
        F.trim(F.col("s.customer")).alias("customer"),
        F.trim(F.col("s.customer_name")).alias("customer_name"),
        F.trim(F.col("s.customer_group")).alias("customer_group"),
        F.trim(F.col("s.territory")).alias("territory"),
        F.trim(F.col("s.company")).alias("company"),

        F.to_date("s.posting_date").alias("posting_date"),
        F.to_date("s.due_date").alias("due_date"),

        "s.is_pos",
        "s.is_return",
        "s.docstatus",

        F.trim(F.col("s.currency")).alias("currency"),
        "s.conversion_rate",
        "s.total_qty",
        "s.net_total",
        "s.grand_total",
        "s.rounded_total",
        "s.outstanding_amount",
        "s.paid_amount",

        F.trim(F.col("s.apply_discount_on")).alias("apply_discount_on"),
        "s.discount_amount",
        "s.additional_discount_percentage",

        F.trim(F.col("s.customer_address")).alias("customer_address"),
        F.trim(F.col("s.shipping_address")).alias("shipping_address"),

        F.trim(F.col("s.status")).alias("status"),
        "s.is_internal_customer",
        "s.is_discounted",
        "s.remarks",

        # KEEP arrays
        "s.items",
        "s.payments",
        "s.payment_schedule",
        "s.sales_team",
        "s.taxes",

        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 8. Prevent Empty Write
    # -------------------------------------
    if silver_df.rdd.isEmpty():
        print("❌ Silver DF is EMPTY — nothing to write!")
        return

    print(f"✅ Final row count: {silver_df.count()}")

    # -------------------------------------
    # 9. Write to Silver
    # -------------------------------------
    try:
        (silver_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )
        print("✅ Sales Invoice Silver written successfully")

    except Exception as e:
        print(f"❌ Write failed: {str(e)}")
        raise

    print(f"📁 Silver path → {silver_path}")
    silver_df.show(50, truncate=False)