#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


# In[2]:


credentials_location = '../../dtc-gc-37b8d03b4e65.json'

conf = SparkConf()     .setMaster('local[*]')     .setAppName('eia')     .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar")     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")


# In[3]:


spark = SparkSession.builder     .config(conf=sc.getConf())     .getOrCreate()


# In[4]:


import pandas as pd


# In[5]:


from pyspark.sql import types


# In[6]:


production_schema = types.StructType([
    types.StructField("state", types.StringType(), True),
    types.StructField("year", types.IntegerType(), True),
    types.StructField("annual_average", types.FloatType(), True),
    types.StructField("annual_total", types.FloatType(), True),
    types.StructField("state_category", types.StringType(), True)
])


# In[10]:


period = 'week'
print(f'processing data for {period}s...')

input_path = f'gs://dtc_data_lake_dtc-gc/data/eia/{period}/*/'
output_path = f'gs://dtc_data_lake_dtc-gc/pq/eia/{period}/'

df_production = spark.read  .option("header", "true")  .schema(production_schema)  .csv(input_path)

df_production = df_production.withColumn("year", input_file_name())
df_production = df_production.withColumn("year", regexp_extract("year", "(\d{4})", 1))

df_production   .repartition(4)   .write.parquet(output_path, mode='overwrite')



# In[42]:


#df_production.dtypes


# In[45]:


#df_production.show()


# In[ ]:




