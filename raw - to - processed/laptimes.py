# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on lap_times files

# COMMAND ----------

# DBTITLE 1,Importing functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Creating schema
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# DBTITLE 1,Reading folder
df_lap_times = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/adlsformula1/raw/lap_times")

# COMMAND ----------

# DBTITLE 1,Renaming column and creating new column
df_lap_times = df_lap_times.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("date_load", current_timestamp())

# COMMAND ----------

display(df_lap_times)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
df_lap_times.write.mode("overwrite").parquet("/mnt/adlsformula1/processed/lap_times")
