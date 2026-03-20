import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
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
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_path = "/opt/airflow/data/Bronze/delta/Sales_Invoice"
    silver_path = "/opt/airflow/data/Silver/delta/Sales_Invoice"

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
    # 3. Parse JSON
    # -------------------------------------
    # Store all header-level columns as Struct
    header_schema = StructType([
        StructField("name", StringType()),
        StructField("owner", StringType()),
        StructField("creation", StringType()),
        StructField("modified", StringType()),
        StructField("modified_by", StringType()),
        StructField("docstatus", IntegerType()),
        StructField("idx", IntegerType()),
        StructField("title", StringType()),
        StructField("naming_series", StringType()),
        StructField("customer", StringType()),
        StructField("customer_name", StringType()),
        StructField("company", StringType()),
        StructField("posting_date", StringType()),
        StructField("posting_time", StringType()),
        StructField("set_posting_time", IntegerType()),
        StructField("due_date", StringType()),
        StructField("is_pos", IntegerType()),
        StructField("is_consolidated", IntegerType()),
        StructField("is_return", IntegerType()),
        StructField("update_outstanding_for_self", IntegerType()),
        StructField("update_billed_amount_in_sales_order", IntegerType()),
        StructField("update_billed_amount_in_delivery_note", IntegerType()),
        StructField("is_debit_note", IntegerType()),
        StructField("currency", StringType()),
        StructField("conversion_rate", DoubleType()),
        StructField("selling_price_list", StringType()),
        StructField("price_list_currency", StringType()),
        StructField("plc_conversion_rate", DoubleType()),
        StructField("ignore_pricing_rule", IntegerType()),
        StructField("update_stock", IntegerType()),
        StructField("total_qty", DoubleType()),
        StructField("total_net_weight", DoubleType()),
        StructField("custom_total_buying_price", DoubleType()),
        StructField("custom_total_margin_percentage", DoubleType()),
        StructField("base_total", DoubleType()),
        StructField("base_net_total", DoubleType()),
        StructField("custom_total_margin_amount", DoubleType()),
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
        StructField("use_company_roundoff_cost_center", IntegerType()),
        StructField("rounded_total", DoubleType()),
        StructField("in_words", StringType()),
        StructField("total_advance", DoubleType()),
        StructField("outstanding_amount", DoubleType()),
        StructField("disable_rounded_total", IntegerType()),
        StructField("apply_discount_on", StringType()),
        StructField("base_discount_amount", DoubleType()),
        StructField("is_cash_or_non_trade_discount", IntegerType()),
        StructField("additional_discount_percentage", DoubleType()),
        StructField("discount_amount", DoubleType()),
        StructField("total_billing_hours", DoubleType()),
        StructField("total_billing_amount", DoubleType()),
        StructField("base_paid_amount", DoubleType()),
        StructField("paid_amount", DoubleType()),
        StructField("base_change_amount", DoubleType()),
        StructField("change_amount", DoubleType()),
        StructField("allocate_advances_automatically", IntegerType()),
        StructField("only_include_allocated_payments", IntegerType()),
        StructField("write_off_amount", DoubleType()),
        StructField("base_write_off_amount", DoubleType()),
        StructField("write_off_outstanding_amount_automatically", IntegerType()),
        StructField("redeem_loyalty_points", IntegerType()),
        StructField("loyalty_points", IntegerType()),
        StructField("loyalty_amount", DoubleType()),
        StructField("customer_address", StringType()),
        StructField("address_display", StringType()),
        StructField("territory", StringType()),
        StructField("shipping_address_name", StringType()),
        StructField("shipping_address", StringType()),
        StructField("company_address", StringType()),
        StructField("company_address_display", StringType()),
        StructField("ignore_default_payment_terms_template", IntegerType()),
        StructField("po_no", StringType()),
        StructField("debit_to", StringType()),
        StructField("party_account_currency", StringType()),
        StructField("is_opening", StringType()),
        StructField("against_income_account", StringType()),
        StructField("amount_eligible_for_commission", DoubleType()),
        StructField("commission_rate", DoubleType()),
        StructField("total_commission", DoubleType()),
        StructField("letter_head", StringType()),
        StructField("group_same_items", IntegerType()),
        StructField("language", StringType()),
        StructField("status", StringType()),
        StructField("customer_group", StringType()),
        StructField("is_internal_customer", IntegerType()),
        StructField("is_discounted", IntegerType()),
        StructField("remarks", StringType()),
        StructField("repost_required", IntegerType()),
        StructField("doctype", StringType()),

        # Nested arrays stored as ARRAY<STRING> for now
        StructField("sales_team", ArrayType(StringType())),
        StructField("packed_items", ArrayType(StringType())),
        StructField("timesheets", ArrayType(StringType())),
        StructField("pricing_rules", ArrayType(StringType())),
        StructField("taxes", ArrayType(StringType())),
        StructField("payment_schedule", ArrayType(StringType())),
        StructField("payments", ArrayType(StringType())),
        StructField("items", ArrayType(StringType())),
        StructField("advances", ArrayType(StringType()))
    ])

    # -------------------------------------
    # 4. Expand JSON
    # -------------------------------------
    df = latest_df.select(F.explode(F.from_json(F.col("data"), ArrayType(header_schema))).alias("s"), 
                          "batchid", "creationdate", "md5")

    # -------------------------------------
    # 5. Flatten header columns
    # -------------------------------------
    silver_df = df.select(
        "s.*",
        "batchid",
        "creationdate",
        "md5"
    )

    # -------------------------------------
    # 6. Explode nested arrays to get items × payments × payment_schedule
    # -------------------------------------
    silver_df = (silver_df
        .withColumn("item", F.explode_outer("items"))
        .withColumn("payment", F.explode_outer("payments"))
        .withColumn("payment_schedule_row", F.explode_outer("payment_schedule"))
        .withColumn("sales_team_row", F.explode_outer("sales_team"))
    )

    # -------------------------------------
    # 7. Write Silver
    # -------------------------------------
    try:
        (silver_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("replaceWhere", "1=1")
            .save(silver_path)
        )
        print("✅ Sales Invoice Silver written")
    except Exception as e:
        if "concurrent" in str(e).lower():
            import time
            time.sleep(2)
            (silver_df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(silver_path)
            )
            print("✅ Retry success")
        else:
            raise

    print(f"Silver path → {silver_path}")
    silver_df.show(5, truncate=False)