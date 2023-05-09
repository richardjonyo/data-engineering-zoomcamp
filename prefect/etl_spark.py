#This ETL runs a Spark Job that fetches the raw CSV files from GCS and saves them as Parquet file on GCS.

import pyspark
import re
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types


@flow(log_prints=True)
def etl_parent_flow(period:str):
    """Main ETL Flow for Spark job - creates a schema and load parquet files to GCS """  
    
    credentials_location = 'google/dtc-gc-37b8d03b4e65.json'
    conf = SparkConf().setMaster('local[*]').setAppName('eia').set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar")     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

    production_schema = types.StructType([
        types.StructField("state", types.StringType(), True),
        types.StructField("year", types.IntegerType(), True),
        types.StructField("week_1", types.IntegerType(), True),
        types.StructField("week_2", types.IntegerType(), True),
        types.StructField("week_3", types.IntegerType(), True),
        types.StructField("week_4", types.IntegerType(), True),
        types.StructField("week_5", types.IntegerType(), True),
        types.StructField("week_6", types.IntegerType(), True),
        types.StructField("week_7", types.IntegerType(), True),
        types.StructField("week_8", types.IntegerType(), True),
        types.StructField("week_9", types.IntegerType(), True),
        types.StructField("week_10", types.IntegerType(), True),
        types.StructField("week_11", types.IntegerType(), True),
        types.StructField("week_12", types.IntegerType(), True),
        types.StructField("week_13", types.IntegerType(), True),
        types.StructField("week_14", types.IntegerType(), True),
        types.StructField("week_15", types.IntegerType(), True),
        types.StructField("week_16", types.IntegerType(), True),
        types.StructField("week_17", types.IntegerType(), True),
        types.StructField("week_18", types.IntegerType(), True),
        types.StructField("week_19", types.IntegerType(), True),
        types.StructField("week_20", types.IntegerType(), True),
        types.StructField("week_21", types.IntegerType(), True),
        types.StructField("week_22", types.IntegerType(), True),
        types.StructField("week_23", types.IntegerType(), True),
        types.StructField("week_24", types.IntegerType(), True),
        types.StructField("week_25", types.IntegerType(), True),
        types.StructField("week_26", types.IntegerType(), True),
        types.StructField("week_27", types.IntegerType(), True),
        types.StructField("week_28", types.IntegerType(), True),
        types.StructField("week_29", types.IntegerType(), True),
        types.StructField("week_30", types.IntegerType(), True),
        types.StructField("week_31", types.IntegerType(), True),
        types.StructField("week_32", types.IntegerType(), True),
        types.StructField("week_33", types.IntegerType(), True),
        types.StructField("week_34", types.IntegerType(), True),
        types.StructField("week_35", types.IntegerType(), True),
        types.StructField("week_36", types.IntegerType(), True),
        types.StructField("week_37", types.IntegerType(), True),
        types.StructField("week_38", types.IntegerType(), True),
        types.StructField("week_39", types.IntegerType(), True),
        types.StructField("week_40", types.IntegerType(), True),
        types.StructField("week_41", types.IntegerType(), True),
        types.StructField("week_42", types.IntegerType(), True),
        types.StructField("week_43", types.IntegerType(), True),
        types.StructField("week_44", types.IntegerType(), True),
        types.StructField("week_45", types.IntegerType(), True),
        types.StructField("week_46", types.IntegerType(), True),
        types.StructField("week_47", types.IntegerType(), True),
        types.StructField("week_48", types.IntegerType(), True),
        types.StructField("week_49", types.IntegerType(), True),
        types.StructField("week_50", types.IntegerType(), True),
        types.StructField("week_51", types.IntegerType(), True),
        types.StructField("week_52", types.IntegerType(), True),
        types.StructField("week_53", types.IntegerType(), True),            
        types.StructField("annual_average", types.FloatType(), True),
        types.StructField("annual_total", types.FloatType(), True)
    ])

    print(f'processing data for {period}s...')
    input_path = f'gs://dtc_data_lake_dtc-gc/data/eia/{period}/*/'
    output_path = f'gs://dtc_data_lake_dtc-gc/pq/eia/{period}/'

    # partition the parquet files 
    df_production = spark.read  .option("header", "true")  .schema(production_schema)  .csv(input_path)
    df_production = df_production.withColumn("year", input_file_name())
    df_production = df_production.withColumn("year", regexp_extract("year", "(\d{4})", 1))
    df_production   .repartition(4)   .write.parquet(output_path, mode='overwrite')

if __name__ == '__main__':
    period = "week" #'week' or 'month' 
    etl_parent_flow(period)
