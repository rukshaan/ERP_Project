import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
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

    if not os.path.exists(bronze_path):
        raise Exception(f"Bronze path not found: {bronze_path}")

    # -------------------------------------
    # 2. Read latest batch
    # -------------------------------------
    bronze_df = spark.read.format("delta").load(bronze_path)

    latest_df = (
        bronze_df
        .filter(F.col("islatest") == True)
        .orderBy(F.col("creationdate").desc())
        .limit(1)
    )

    # -------------------------------------
    # 3. FULL QUOTATION SCHEMA
    # -------------------------------------
    quotation_schema = ArrayType(
        StructType([

            # Top-level fields
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

            # Nested arrays (will explode later)
            StructField("items", ArrayType(MapType(StringType(), StringType(), True))),
            StructField("payments", ArrayType(MapType(StringType(), StringType(), True))),
            StructField("payment_schedule", ArrayType(MapType(StringType(), StringType(), True))),
            StructField("taxes", ArrayType(StringType())),
            StructField("packed_items", ArrayType(StringType())),
            StructField("competitors", ArrayType(StringType())),
            StructField("lost_reasons", ArrayType(StringType())),
            StructField("pricing_rules", ArrayType(StringType()))

        ])
    )

    # -------------------------------------
    # 4. Expand JSON
    # -------------------------------------
    df = latest_df.select(
        F.explode(F.from_json(F.col("data"), quotation_schema)).alias("q"),
        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 5. Explode nested arrays for cross product
    # -------------------------------------
    df = df.withColumn("item", F.explode_outer("q.items")) \
           .withColumn("payment", F.explode_outer("q.payments")) \
           .withColumn("schedule", F.explode_outer("q.payment_schedule"))

    # -------------------------------------
    # 6. Flatten all columns and rename nested fields
    # -------------------------------------
    # Since item, payment, schedule are Maps (not Structs), we need to extract keys differently
    # First, let's get all possible keys from the maps
    
    # For Maps, we need to explicitly select the keys we want
    # Common ERPNext item fields
    item_fields = [
        "item_code", "item_name", "description", "qty", "rate", "amount",
        "warehouse", "uom", "stock_qty", "stock_uom", "conversion_factor",
        "price_list_rate", "discount_percentage", "net_rate", "net_amount",
        "billed_amt", "valuation_rate", "gross_profit", "weight_per_unit",
        "weight_uom", "shelf_life", "country_of_origin", "custom_delivery_date"
    ]
    
    # Payment fields
    payment_fields = [
        "mode_of_payment", "amount", "base_amount", "type"
    ]
    
    # Payment schedule fields
    schedule_fields = [
        "payment_term", "due_date", "invoice_portion", "payment_amount",
        "outstanding", "paid_amount", "discount"
    ]
    
    # Build column expressions for map extraction
    item_cols = [F.col("item").getItem(f).alias(f"item_{f}") for f in item_fields]
    payment_cols = [F.col("payment").getItem(f).alias(f"payment_{f}") for f in payment_fields]
    schedule_cols = [F.col("schedule").getItem(f).alias(f"schedule_{f}") for f in schedule_fields]

    silver_df = df.select(
        "q.*",
        *item_cols,
        *payment_cols,
        *schedule_cols,
        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 7. Write to Silver
    # -------------------------------------
    try:
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("replaceWhere", "1=1") \
            .save(silver_path)
        print("✅ Quotation Silver table written successfully")
    except Exception as e:
        if "concurrent" in str(e).lower():
            import time
            time.sleep(2)
            silver_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(silver_path)
            print("✅ Retry success")
        else:
            raise

    print(f"Silver path → {silver_path}")
    silver_df.show(5, truncate=False)