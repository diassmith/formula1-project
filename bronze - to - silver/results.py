# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on results.json file

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraries and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, lit

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
.json(f"{bronze_folder_path}/results.json")

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Renaming the column and creating new column
df_results = df_results.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# DBTITLE 1,Creating new column
df_results = add_date_load(df_results)

# COMMAND ----------

# DBTITLE 1,Droping column
df_results = df_results.drop(col("statusId"))

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Write output parquet file with partition by race_id
df_results.write.mode("overwrite").partitionBy('race_id').parquet(f"{silver_folder_path}/results")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
