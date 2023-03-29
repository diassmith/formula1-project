# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_date_load_bronze(input_df):
  df_output = input_df.withColumn("date_load_bronze", current_timestamp())
  return df_output

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_date_load_silver(input_df):
  df_output = input_df.withColumn("date_load_silver", current_timestamp())
  return df_output
