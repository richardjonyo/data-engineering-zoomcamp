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
def etl_parent_flow(period: str):
    """Main ETL Flow for Spark job - creates a schema and load parquet files to GCS """  
    
    credentials_location = 'google/dtc-gc-37b8d03b4e65.json'
    conf = SparkConf().setMaster('local[*]').setAppName('eia').set("spark.jars", "./lib/gcs-connector-hadoop3-latest.jar")     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
    
    field_names = ["state", "year"] + [f"week_{i}" for i in range(1, 54)] + ["annual_average", "annual_total"]
    field_types = [types.StringType(), types.IntegerType()] + [types.IntegerType() for _ in range(1, 54)] + [types.FloatType(), types.FloatType()]
    production_schema = types.StructType([types.StructField(name, field_type, True) for name, field_type in zip(field_names, field_types)])
    print(production_schema)
    print(f'processing data for {period}s...')
    input_path = f'gs://dtc_data_lake_dtc-gc/data/pq/{period}/*/'
    output_path = f'gs://dtc_data_lake_dtc-gc/spark/{period}/'

    # partition the parquet files 
    df_production = spark.read  .option("header", "true")  .schema(production_schema)  .csv(input_path)
    df_production = df_production.withColumn("year", input_file_name())
    df_production = df_production.withColumn("year", regexp_extract("year", "(\d{4})", 1))
    df_production.repartition(4).write.parquet(output_path, mode='overwrite')

if __name__ == '__main__':
    period = "week" #'week' or 'month' 
    etl_parent_flow(period)
