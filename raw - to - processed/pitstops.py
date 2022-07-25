# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on pit_stops.json file

# COMMAND ----------

# DBTITLE 1,Importing libraies and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

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
.json("/mnt/adlsformula1/raw/pit_stops.json")

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Renaming columns
df_pit_stops = df_pit_stops.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("data_load", current_timestamp())

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
df_pit_stops.write.mode("overwrite").parquet("/mnt/adlsformula1/processed/pit_stops")
