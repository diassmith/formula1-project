# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on pit_stops.json file

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraies and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# DBTITLE 1,Creating Schema
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# DBTITLE 1,Reading file
df_pit_stops = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Renaming columns
df_pit_stops = df_pit_stops.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

# DBTITLE 1,Creating new column
df_pit_stops = add_date_load(df_pit_stops)

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
df_pit_stops.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
