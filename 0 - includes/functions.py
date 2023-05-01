# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, asc,desc, monotonically_increasing_id, concat, lit, max, min, row_number, hash, abs,dayofyear, year
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import date, datetime
import pytz

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp
from pytz import timezone

# COMMAND ----------

def add_date_load_landing(input_df):
    sao_paulo_tz = timezone('America/Sao_Paulo')
    current_time = current_timestamp()
    current_time_sp = from_utc_timestamp(current_time, sao_paulo_tz.zone)
    df_output = input_df.withColumn("date_ref", current_time_sp)
    return df_output

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp
# def add_date_load_landing(input_df):
#   df_output = input_df.withColumn("date_ref", current_timestamp())
#   return df_output

# COMMAND ----------

def add_date_load_bronze(input_df):
    sao_paulo_tz = timezone('America/Sao_Paulo')
    current_time = current_timestamp()
    current_time_sp = from_utc_timestamp(current_time, sao_paulo_tz.zone)
    df_output = input_df.withColumn("date_load_bronze", current_time_sp)
    return df_output

# COMMAND ----------


def add_date_load_silver(input_df):
    sao_paulo_tz = timezone('America/Sao_Paulo')
    current_time = current_timestamp()
    current_time_sp = from_utc_timestamp(current_time, sao_paulo_tz.zone)
    df_output = input_df.withColumn("date_load_silver", current_time_sp)
    return df_output

# COMMAND ----------

def add_date_load_gold(input_df):
    sao_paulo_tz = timezone('America/Sao_Paulo')
    current_time = current_timestamp()
    current_time_sp = from_utc_timestamp(current_time, sao_paulo_tz.zone)
    df_output = input_df.withColumn("date_load_gold", current_time_sp)
    return df_output

# COMMAND ----------

def re_arrange_partition_column(df_input, partition_column):
    column_list = []
    for column_name in df_input.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    df_output = df_input.select(column_list)
    return df_output

# COMMAND ----------

def overwrite_partition(df_input, db_name, table_name, partition_column):
    df_output = re_arrange_partition_column(df_input, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        df_output.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        df_output.write.mode("overwrite").partitionBy(partition_column).format(
            "parquet"
        ).saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def upsert(df_target, targetKey,df_landing, landingKey):
    (df_target.alias("target")
     .merge(df_landing.alias("updates"), "(target."+targetKey+"= updates."+landingKey+") and (target.date_ref <> updates.date_ref)")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )

# COMMAND ----------

def upsert2(df_target, targetKey,targetKey2,df_landing, landingKey,landingKey2):
    (df_target.alias("target")
     .merge(df_landing.alias("updates"), "(target."+targetKey+"= updates."+landingKey+") and (target."+targetKey2+"= updates."+landingKey2+")  and (target.file_date <> updates.file_date)")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
