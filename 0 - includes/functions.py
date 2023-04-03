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
