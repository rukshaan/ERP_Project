import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.types import *


def transform_quotation_to_silver(**kwargs):

    # -------------------------------------
    # 1. Spark Session
    # -------------------------------------
    builder = (
        SparkSession.builder
        .appName("QuotationSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.debug.maxToStringFields", "100")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_path = "/opt/airflow/data/Bronze/delta/Quotation"
    silver_path = "/opt/airflow/data/Silver/delta/Quotation"

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

    if bronze_df.limit(2).count() == 0:
        print("⚠️ Bronze table is empty")
        return

    # -------------------------------------
    # 3. Get Latest Batch (SAFE)
    # -------------------------------------
    latest_batch_row = (
        bronze_df
        .filter(F.col("islatest") == True)
        .select("batchid")
        .orderBy(F.col("batchid").desc())
        .limit(1)
        .collect()
    )

    if not latest_batch_row:
        print("⚠️ No latest batch found")
        return

    latest_batch_id = latest_batch_row[0]["batchid"]

    latest_df = bronze_df.filter(F.col("batchid") == latest_batch_id)

    print(f"✅ Processing batch: {latest_batch_id}")

    # -------------------------------------
    # 4. ITEM SCHEMA (UNCHANGED LOGIC)
    # -------------------------------------
    items_schema = ArrayType(
        StructType([
            StructField("item_code", StringType()),
            StructField("item_name", StringType()),
            StructField("description", StringType())
        ])
    )

    # -------------------------------------
    # 5. FULL QUOTATION SCHEMA (ALL COLUMNS KEPT)
    # -------------------------------------
    quotation_schema = StructType([

        StructField("name", StringType()),
        StructField("owner", StringType()),
        StructField("creation", StringType()),
        StructField("modified", StringType()),
        StructField("modified_by", StringType()),
        StructField("docstatus", IntegerType()),
        StructField("idx", IntegerType()),
        StructField("title", StringType()),
        StructField("naming_series", StringType()),
        StructField("quotation_to", StringType()),
        StructField("party_name", StringType()),
        StructField("customer_name", StringType()),
        StructField("transaction_date", StringType()),
        StructField("valid_till", StringType()),
        StructField("custom_delivery_date", StringType()),
        StructField("order_type", StringType()),
        StructField("company", StringType()),
        StructField("currency", StringType()),
        StructField("conversion_rate", DoubleType()),
        StructField("selling_price_list", StringType()),
        StructField("price_list_currency", StringType()),
        StructField("plc_conversion_rate", DoubleType()),
        StructField("ignore_pricing_rule", IntegerType()),
        StructField("total_qty", DoubleType()),
        StructField("total_net_weight", DoubleType()),
        StructField("base_total", DoubleType()),
        StructField("base_net_total", DoubleType()),
        StructField("custom_total_buying_price", DoubleType()),
        StructField("custom_total_margin_amount", DoubleType()),
        StructField("custom_total_margin_percentage", DoubleType()),
        StructField("total", DoubleType()),
        StructField("net_total", DoubleType()),
        StructField("tax_category", StringType()),
        StructField("base_total_taxes_and_charges", DoubleType()),
        StructField("total_taxes_and_charges", DoubleType()),
        StructField("base_grand_total", DoubleType()),
        StructField("base_rounding_adjustment", DoubleType()),
        StructField("base_rounded_total", DoubleType()),
        StructField("base_in_words", StringType()),
        StructField("grand_total", DoubleType()),
        StructField("rounding_adjustment", DoubleType()),
        StructField("rounded_total", DoubleType()),
        StructField("in_words", StringType()),
        StructField("apply_discount_on", StringType()),
        StructField("base_discount_amount", DoubleType()),
        StructField("additional_discount_percentage", DoubleType()),
        StructField("discount_amount", DoubleType()),
        StructField("customer_address", StringType()),
        StructField("address_display", StringType()),
        StructField("shipping_address_name", StringType()),
        StructField("shipping_address", StringType()),
        StructField("letter_head", StringType()),
        StructField("group_same_items", IntegerType()),
        StructField("language", StringType()),
        StructField("status", StringType()),
        StructField("customer_group", StringType()),
        StructField("territory", StringType()),
        StructField("doctype", StringType()),

        StructField("items", items_schema),

        StructField("payments", ArrayType(StringType())),
        StructField("payment_schedule", ArrayType(StringType())),
        StructField("taxes", ArrayType(StringType())),
        StructField("packed_items", ArrayType(StringType())),
        StructField("competitors", ArrayType(StringType())),
        StructField("lost_reasons", ArrayType(StringType())),
        StructField("pricing_rules", ArrayType(StringType()))
    ])

    # -------------------------------------
    # 6. PARSE JSON (SAFE)
    # -------------------------------------
    parsed_df = latest_df.withColumn(
        "parsed",
        F.from_json(F.col("data"), ArrayType(quotation_schema))
    ).filter(F.col("parsed").isNotNull())

    # -------------------------------------
    # 7. EXPLODE HEADER
    # -------------------------------------
    df = parsed_df.select(
        F.explode_outer("parsed").alias("s"),
        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 8. EXPLODE ITEMS
    # -------------------------------------
    df = df.withColumn("item", F.explode_outer("s.items"))

    # -------------------------------------
    # 9. FLATTEN (ALL COLUMNS PRESERVED)
    # -------------------------------------
    silver_df = df.select(

        # HEADER (ALL FIELDS KEPT)
        F.col("s.name").alias("name"),
        F.col("s.owner").alias("owner"),
        F.col("s.creation").alias("creation"),
        F.col("s.modified").alias("modified"),
        F.col("s.modified_by").alias("modified_by"),
        F.col("s.docstatus").alias("docstatus"),
        F.col("s.idx").alias("idx"),
        F.col("s.title").alias("title"),
        F.col("s.naming_series").alias("naming_series"),
        F.col("s.quotation_to").alias("quotation_to"),
        F.col("s.party_name").alias("party_name"),
        F.col("s.customer_name").alias("customer_name"),
        F.to_date("s.transaction_date").alias("transaction_date"),
        F.to_date("s.valid_till").alias("valid_till"),
        F.col("s.custom_delivery_date").alias("custom_delivery_date"),
        F.col("s.order_type").alias("order_type"),
        F.col("s.company").alias("company"),
        F.col("s.currency").alias("currency"),
        F.col("s.conversion_rate").alias("conversion_rate"),
        F.col("s.selling_price_list").alias("selling_price_list"),
        F.col("s.price_list_currency").alias("price_list_currency"),
        F.col("s.plc_conversion_rate").alias("plc_conversion_rate"),
        F.col("s.ignore_pricing_rule").alias("ignore_pricing_rule"),
        F.col("s.total_qty").alias("total_qty"),
        F.col("s.total_net_weight").alias("total_net_weight"),
        F.col("s.base_total").alias("base_total"),
        F.col("s.base_net_total").alias("base_net_total"),
        F.col("s.custom_total_buying_price").alias("custom_total_buying_price"),
        F.col("s.custom_total_margin_amount").alias("custom_total_margin_amount"),
        F.col("s.custom_total_margin_percentage").alias("custom_total_margin_percentage"),
        F.col("s.total").alias("total"),
        F.col("s.net_total").alias("net_total"),
        F.col("s.tax_category").alias("tax_category"),
        F.col("s.base_total_taxes_and_charges").alias("base_total_taxes_and_charges"),
        F.col("s.total_taxes_and_charges").alias("total_taxes_and_charges"),
        F.col("s.base_grand_total").alias("base_grand_total"),
        F.col("s.base_rounding_adjustment").alias("base_rounding_adjustment"),
        F.col("s.base_rounded_total").alias("base_rounded_total"),
        F.col("s.base_in_words").alias("base_in_words"),
        F.col("s.grand_total").alias("grand_total"),
        F.col("s.rounding_adjustment").alias("rounding_adjustment"),
        F.col("s.rounded_total").alias("rounded_total"),
        F.col("s.in_words").alias("in_words"),
        F.col("s.apply_discount_on").alias("apply_discount_on"),
        F.col("s.base_discount_amount").alias("base_discount_amount"),
        F.col("s.additional_discount_percentage").alias("additional_discount_percentage"),
        F.col("s.discount_amount").alias("discount_amount"),
        F.col("s.customer_address").alias("customer_address"),
        F.col("s.address_display").alias("address_display"),
        F.col("s.shipping_address_name").alias("shipping_address_name"),
        F.col("s.shipping_address").alias("shipping_address"),
        F.col("s.letter_head").alias("letter_head"),
        F.col("s.group_same_items").alias("group_same_items"),
        F.col("s.language").alias("language"),
        F.col("s.status").alias("status"),
        F.col("s.customer_group").alias("customer_group"),
        F.col("s.territory").alias("territory"),
        F.col("s.doctype").alias("doctype"),

        # ITEMS (ALL KEPT)
        F.col("item.item_code").alias("item_code"),
        F.col("item.item_name").alias("item_name"),
        F.col("item.description").alias("item_description"),

        # META
        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 10. VALIDATION
    # -------------------------------------
    if silver_df.limit(2).count() == 0:
        print("❌ Silver DF is EMPTY")
        return

    print(f"✅ Final row count: {silver_df.count()}")

    # -------------------------------------
    # 11. WRITE
    # -------------------------------------
    (silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
    silver_df.show(20, truncate=False)
    print("✅ Quotation Silver written successfully")