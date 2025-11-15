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



def say_hello(**kwargs):
    

    # consfigure a spark session
    builder = SparkSession.builder \
        .appName("SalesOrderDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()


    # get today's date
    today = datetime.datetime.today()
    year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")
    creationdate = today.strftime("%Y-%m-%d %H:%M:%S")

    # Initialize API
    api = main.API()
    doctype = "Sales Order"
    run_id = kwargs["run_id"] 
 
    # Fetch customer data
    results = api.get_dataframe(doctype)
    resultsdata = [i for i in results.get('name', [])][:28]
 
    # Fetch details for each customer
    details_of_sales_order = []
    for res in resultsdata:
        details = api.get_doc_sales(doctype, res)
        details_of_sales_order.append(details)
 
    if not details_of_sales_order:
        raise Exception("No data found in results")
    print("Fetched details for sales orders:", details_of_sales_order)
     # -------- Save to JSON file --------
    
    # Create output directory based on execution date
    exec_date = kwargs["logical_date"]
    date_part = exec_date.strftime("%Y/%m/%d")

    # Define base directory and output path
    base_dir = "/opt/airflow/data/Bronze/Volumes"
    output_dir = os.path.join(base_dir, date_part)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{run_id}_{doctype}.json")

    # write details to JSON file to the output path
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(details_of_sales_order, f, ensure_ascii=False, indent=2, default=str)

    # -------- Save to Delta Lake --------
    # Prepare DataFrame for Delta Lake
    json_string = json.dumps(details_of_sales_order, sort_keys=True)
    md5_hash = hashlib.md5(json_string.encode("utf-8")).hexdigest()
    new_df = pd.DataFrame([{
    "data": json_string,
    "md5": md5_hash,
    "batchid": run_id,
    "creationdate": creationdate,
    "islatest": True
}])
    # create a dataframe with spark
    spark_df = spark.createDataFrame(new_df)
    # spark_df.show()

    delta_path = "/opt/airflow/data/Bronze/delta/SalesOrder"

    spark_df.write.format("delta").mode("append").save(delta_path)
    
    read_df = spark.read.format("delta").load(delta_path)
    read_df.show(truncate=False)
    


    

    print(f"Saved {len(details_of_sales_order)} sales orders to {output_path}")
    return output_path