# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  df_output = input_df.withColumn("date_load", current_timestamp())
  return df_output
