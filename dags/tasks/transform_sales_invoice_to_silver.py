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

    if bronze_df.limit(1).count() == 0:
        print("⚠️ Bronze table is empty")
        return

    # -------------------------------------
    # 3. Get Latest Batch
    # -------------------------------------
    latest_batch_id = (
        bronze_df
        .filter(F.col("islatest") == True)
        .agg(F.max("batchid").alias("max_batch"))
        .collect()[0]["max_batch"]
    )

    latest_df = bronze_df.filter(F.col("batchid") == latest_batch_id)

    print(f"✅ Processing batch: {latest_batch_id}")

    # -------------------------------------
    # 4. Define Schema
    # -------------------------------------
    items_schema = ArrayType(
        StructType([
            StructField("item_code", StringType()),
            StructField("item_name", StringType()),
            StructField("description", StringType()),
            StructField("qty", DoubleType()),
            StructField("rate", DoubleType()),
            StructField("amount", DoubleType()),
            StructField("name", StringType()),
            StructField("item_group", StringType()),
            StructField("image", StringType()),
            StructField("uom", StringType()),
            StructField("custom_margin_amount", DoubleType()),
            StructField("custom_margin_percentage", DoubleType()),
            StructField("stock_qty", DoubleType()),
            StructField("price_list_rate", DoubleType()),
            StructField("base_price_list_rate", DoubleType()),
            StructField("discount_percentage", DoubleType()),
            StructField("sales_order", StringType()),
            StructField("so_detail", StringType()),
            StructField("expense_account", StringType()),
            StructField("cost_center", StringType()),
            StructField("delivered_qty", DoubleType()),
            StructField("incoming_rate", DoubleType()),
            StructField("total_weight", DoubleType()),
        
        ])
    )
    payment_schema = ArrayType(
        StructType([
            StructField("payment_method", StringType()),
            StructField("amount", DoubleType()),
            StructField("date", StringType()),
            StructField("invoice_portion", DoubleType()),
            StructField("due_date", StringType()),
            StructField("name", StringType()),
            StructField("payment_amount", DoubleType()),
        ])
    )
    sales_team_schema = ArrayType(
        StructType([
            StructField("name", StringType()),
            StructField("sales_person", StringType()),
            StructField("allocated_amount", DoubleType()),
            StructField("allocated_percentage", DoubleType()),

        ])
    )

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
        StructField("items", items_schema),
        StructField("payments", payment_schema),
        StructField("payment_schedule", ArrayType(StringType())),
        StructField("sales_team", sales_team_schema),
        StructField("taxes", ArrayType(StringType()))
    ])

    # -------------------------------------
    # 5. Parse JSON
    # -------------------------------------
    parsed_df = latest_df.withColumn(
        "parsed",
        F.from_json(F.col("data"), ArrayType(header_schema))
    )

    # -------------------------------------
    # 6. Explode HEADER
    # -------------------------------------
    df = parsed_df.select(
        F.explode_outer("parsed").alias("s"),
        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 7. EXPLODE ITEMS (CORRECT PLACE)
    # -------------------------------------
    df = df.withColumn("item", F.explode_outer("s.items"))
    df = df.withColumn("payment", F.explode_outer("s.payments"))
    df = df.withColumn("sales_team", F.explode_outer("s.sales_team"))
    # -------------------------------------
    # 8. FLATTEN
    # -------------------------------------
    silver_df = df.select(

        # HEADER
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

        # ITEM
        F.col("item.item_code").alias("item_code"),
        F.col("item.item_name").alias("item_name"),
        F.col("item.description").alias("item_description"),
        F.col("item.qty").alias("item_qty"),
        F.col("item.rate").alias("item_rate"),
        F.col("item.amount").alias("item_amount"),
        F.col("item.item_group").alias("item_group"),
        F.col("item.image").alias("item_image"),
        F.col("item.uom").alias("item_uom"),
        F.col("item.custom_margin_amount").alias("item_custom_margin_amount"),
        F.col("item.custom_margin_percentage").alias("item_custom_margin_percentage"),
        F.col("item.stock_qty").alias("item_stock_qty"),
        F.col("item.price_list_rate").alias("item_price_list_rate"),
        F.col("item.base_price_list_rate").alias("item_base_price_list_rate"),
        F.col("item.discount_percentage").alias("item_discount_percentage"),
        F.col("item.sales_order").alias("item_sales_order"),
        F.col("item.so_detail").alias("item_so_detail"),
        F.col("item.expense_account").alias("item_expense_account"),
        F.col("item.cost_center").alias("item_cost_center"),
        F.col("item.delivered_qty").alias("item_delivered_qty"),
        F.col("item.incoming_rate").alias("item_incoming_rate"),
        F.col("item.total_weight").alias("item_total_weight"),

        # sales_team_schema
        F.col("sales_team.sales_person").alias("sales_person"),
        F.col("sales_team.allocated_amount").alias("allocated_amount"),
        F.col("sales_team.allocated_percentage").alias("allocated_percentage"),
        F.col("sales_team.name").alias("sales_team_name"),

        # payment_schema
        F.col("payment.payment_method").alias("payment_method"),
        F.col("payment.payment_amount").alias("payment_amount"),
        F.col("payment.date").alias("payment_date"),
        F.col("payment.invoice_portion").alias("payment_invoice_portion"),
        F.col("payment.due_date").alias("payment_due_date"),
        F.col("payment.name").alias("payment_name"),
        
        # META
        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 9. VALIDATE
    # -------------------------------------
    if silver_df.limit(1).count() == 0:
        print("❌ Silver DF is EMPTY")
        return

    print(f"✅ Final row count: {silver_df.count()}")

    # -------------------------------------
    # 10. WRITE (SAFE MODE)
    # -------------------------------------
    (silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    print("✅ Sales Invoice Silver written successfully")
    print(f"📁 Path → {silver_path}")