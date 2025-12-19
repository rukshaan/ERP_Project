from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
 
 
 
def check_schema():
 
    builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
 
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
 
    df = spark.read.format("delta").load("/opt/airflow/data/Silver/delta/Customer")
    # partitions=df.rdd.getNumPartitions()
    # print("Number of partitions:", partitions)
    # df.show()
 
    # print("Number of rows:", df.count())
    column_count = len(df.schema.fields)
    print("Number of columns:", column_count)

    df.printSchema()
 
 
 
    
#    df.write.mode("overwrite").parquet("/opt/airflow/apidata/level3/delta/SalesOrder")
 
    print("✅ Conversion complete!")
 
    spark.stop()
 