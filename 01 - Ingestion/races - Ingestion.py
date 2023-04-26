# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

# COMMAND ----------

response = requests.get('https://ergast.com/api/f1/2023.json')
json_data = response.json()

df_races= spark.createDataFrame(json_data['MRData']['RaceTable']['Races'])

# COMMAND ----------

display(df_races)
