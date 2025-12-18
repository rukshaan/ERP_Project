from . import main
import os
import json
import pyspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
import hashlib
import pandas as pd
from delta import configure_spark_with_delta_pip, DeltaTable
import pyspark.sql.functions as F


def get_sales_data_bronze_dag(**kwargs):

    # ----------------------------------------------------------
    # 1. Configure Spark Session
    # ----------------------------------------------------------
    builder = (
        SparkSession.builder
        .appName("BronzeMultiDoctype")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # ----------------------------------------------------------
    # 2. Setup
    # ----------------------------------------------------------
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    api = main.API()
    doctypes = ["Sales Order", "Customer"]
    run_id = kwargs["run_id"]

    # Base data folders
    base_json_dir = "/opt/airflow/data/Bronze/Volumes"
    base_delta_dir = "/opt/airflow/data/Bronze/delta"

    os.makedirs(base_json_dir, exist_ok=True)
    os.makedirs(base_delta_dir, exist_ok=True)

    # ----------------------------------------------------------
    # 3. Main Loop - Process each doctype
    # ----------------------------------------------------------
    for doctype in doctypes:
        print(f" Processing doctype: {doctype}")

        # Fetch list
        results = api.get_dataframe(doctype)
        if results is None:
            print(f" No response for {doctype}, skipping.")
            continue

        names = [i for i in results.get("name", [])]

        # Fetch full docs
        full_docs = []
        for name in names:
            doc = api.get_doc_sales(doctype, name)
            if doc:
                full_docs.append(doc)

        if not full_docs:
            print(f"⚠ No documents found for {doctype}")
            print(f"⚠ No documents found for {doctype}")
            continue

        # Save raw JSON in Bronze Volume
        json_path = os.path.join(base_json_dir, f"{doctype.replace(' ', '')}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(full_docs, f, ensure_ascii=False, indent=2)

        print(f" JSON saved: {json_path}")

        # Generate md5 hash
        json_string = json.dumps(full_docs, sort_keys=True)
        md5_hash = hashlib.md5(json_string.encode("utf-8")).hexdigest()

        # Convert JSON string to bronze record
        new_df = pd.DataFrame([{
            "data": json_string,
            "md5": md5_hash,
            "batchid": run_id,
            "creationdate": today,
            "islatest": True
        }])

        df_new = spark.createDataFrame(new_df)

        # Delta table path
        delta_path = os.path.join(base_delta_dir, doctype.replace(" ", ""))

        # ------------------------------------------------------
        # CHECK: If Delta table exists
        # ------------------------------------------------------
        if DeltaTable.isDeltaTable(spark, delta_path):
            delta_table = DeltaTable.forPath(spark, delta_path)

            # Check if hash already exists → no change
            existing = delta_table.toDF().filter(F.col("md5") == md5_hash).count()

            if existing > 0:
                print(f" No change for {doctype}, skipping delta update.")
                continue

            # Overwrite with new bronze record
            df_new.write.format("delta").mode("overwrite").save(delta_path)
            print(f"✔ Updated Delta table for {doctype}")

        else:
            # --------------------------------------------------
            # FIRST RUN → CREATE DELTA TABLE
            # --------------------------------------------------
            print(f" Creating Delta table for {doctype} (first run)")
            df_new.write.format("delta").mode("overwrite").save(delta_path)

    print(" Bronze multi-doctype ingestion complete!")
    return "Bronze multi-doctype ingestion complete!"
