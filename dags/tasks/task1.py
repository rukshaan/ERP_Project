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
from delta import configure_spark_with_delta_pip
import datetime
import pyspark.sql.functions as F


def say_hello(**kwargs):

    # -------------------------------------
    # 1. Configure Spark Session
    # -------------------------------------
    builder = (
        SparkSession.builder
        .appName("SalesOrderDelta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # -------------------------------------
    # 2. Setup
    # -------------------------------------
    today = datetime.datetime.today()
    creationdate = today.strftime("%Y-%m-%d %H:%M:%S")

    api = main.API()
    doctype = "Sales Order"
    run_id = kwargs["run_id"]

    # -------------------------------------
    # 3. Fetch ERPNext Sales Order Data
    # -------------------------------------
    results = api.get_dataframe(doctype)
    resultsdata = [i for i in results.get("name", [])][:28]

    details_of_sales_order = []
    for res in resultsdata:
        details = api.get_doc_sales(doctype, res)
        details_of_sales_order.append(details)

    if not details_of_sales_order:
        raise Exception("No data found in results")

    # print("Fetched details for sales orders:", details_of_sales_order)

    # -------------------------------------
    # 4. Save JSON to Bronze/Volumes/YYYY/MM/DD
    # -------------------------------------
    exec_date = kwargs["logical_date"]
    date_part = exec_date.strftime("%Y/%m/%d")

    base_dir = "/opt/airflow/data/Bronze/Volumes"
    # output_dir = os.path.join(base_dir, date_part)
    os.makedirs(base_dir, exist_ok=True)

    output_path = os.path.join(base_dir, f"{doctype}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(details_of_sales_order, f, ensure_ascii=False, indent=2, default=str)

    # -------------------------------------
    # 5. Prepare Data For Delta
    # -------------------------------------
    json_string = json.dumps(details_of_sales_order, sort_keys=True)
    md5_hash = hashlib.md5(json_string.encode("utf-8")).hexdigest()
    new_df = pd.DataFrame([{
                "data": json_string,
                "md5": md5_hash,
                "batchid": run_id,
                "creationdate": creationdate,
                "islatest": True
            }])

    spark_new_df = spark.createDataFrame(new_df)

    delta_path = "/opt/airflow/data/Bronze/delta/SalesOrder"

    # -------------------------------------
    # 6. Load Delta Table (if exists)
    # -------------------------------------
    if os.path.exists(delta_path):
        delta_df = spark.read.format("delta").load(delta_path)
        same_md5 = delta_df.filter(F.col("md5") == md5_hash)

        if same_md5.count() > 0:
            print(" No change detected. MD5 already exists. Skipping Delta insert.")
            print(f"Saved JSON file only → {output_path}")
            return output_path
        else:
            print("Change detected → Updating Delta...")
            updated_old_df = delta_df.withColumn(
            "islatest",
            F.when(F.col("islatest") == True, False).otherwise(F.col("islatest"))
            )

            # -------------------------------------
            # 9. Create new Delta record
            # -------------------------------------
            

            # -------------------------------------
            # 10. Create final merged DF and overwrite Delta
            # -------------------------------------
            

            spark_new_df.write.format("delta").mode("overwrite").save(delta_path)

            print(" Delta updated successfully!")
        
    else:
        spark_new_df.write.format("delta").mode("overwrite").save(delta_path)


    # -------------------------------------
    # 7. CHECK IF SAME MD5 EXISTS
    # -------------------------------------
    

    # -------------------------------------
    # 8. Mark previous latest record as FALSE
    # -------------------------------------
    

    # Show result
    final_read = spark.read.format("delta").load(delta_path)
    final_read.show(truncate=False)

    print(f"Saved {len(details_of_sales_order)} sales orders to {output_path}")
    return output_path
