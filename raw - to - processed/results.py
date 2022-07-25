# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on results.json file

# COMMAND ----------

# DBTITLE 1,Importing libraries and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Creating schema
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

# DBTITLE 1,Reading file
df_results = spark.read\
.schema(results_schema)\
.json("/mnt/adlsformula1/raw/results.json")

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Renaming the column
df_results = df_results.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# DBTITLE 1,Droping column
df_results = df_results.drop(col("statusId"))

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Write output parquet file with partition by race_id
df_results.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/adlsformula1/processed/results")