# Databricks notebook source
import requests
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import date, datetime
import pytz

# COMMAND ----------

# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# Get date in UTC
date_time_utc = datetime.utcnow()

# define the local time
fuso_horario_local = pytz.timezone('America/Sao_Paulo')

# fix the date with local datetime
date_time_local = date_time_utc.replace(tzinfo=pytz.utc).astimezone(fuso_horario_local)

# get the date from datetime
actual_date = date_time_local.date()

# COMMAND ----------

response = requests.get('https://ergast.com/api/f1/circuits.json')
json_data = response.json()

df = spark.createDataFrame(json_data['MRData']['CircuitTable']['Circuits'])

# COMMAND ----------

df = df.select(col('Location').getItem('locality').alias('locality'),
               col('Location').getItem('country').alias('country'),
               col('Location').getItem('lat').alias('lat'), 
               col('Location').getItem('long').alias('long'),
               col('circuitId'), 
               col('circuitName'), 
               col('url'))

# COMMAND ----------

display(df.orderBy("locality"))

# COMMAND ----------

df.write.mode("overwrite").parquet(f"{landing_folder_path}/circuits/"+str(actual_date))
