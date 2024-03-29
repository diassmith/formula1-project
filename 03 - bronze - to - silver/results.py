# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on results.json file

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraries and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Reading file
# df_results = spark.read\
# .schema(results_schema)\
# .json(f"{bronze_folder_path}/results.json")

df_results = spark.read.parquet(f"{bronze_folder_path}/results")

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Renaming the column and creating new column
df_results = df_results.withColumnRenamed("resultId", "result_id")\
                                    .withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumnRenamed("positionText", "position_text")\
                                    .withColumnRenamed("positionOrder", "position_order")\
                                    .withColumnRenamed("fastestLap", "fastest_lap")\
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

# DBTITLE 1,Creating new column
df_results = add_date_load_silver(df_results)

# COMMAND ----------

# DBTITLE 1,Droping column
df_results = df_results.drop(col("statusId"))

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Write output parquet file with partition by race_id
#df_results.write.mode("overwrite").partitionBy('race_id').parquet(f"{silver_folder_path}/results")

# COMMAND ----------

df_results.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.results")

# COMMAND ----------

if spark.catalog.tableExists("f1_silver.results"):
    df_target = DeltaTable.forPath(spark, '/mnt/adlsformula1/silver/results')
    print("upsert")
    upsert(df_target,"result_id",df_results,"result_id")
else:
    print("New")
    df_results.write.mode("overwrite").format("delta").saveAsTable("f1_silver.results")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
