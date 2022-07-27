# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on qualifying json files

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraries and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# DBTITLE 1,Creating schema
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

# DBTITLE 1,Reading folder
df_qualifying = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

# DBTITLE 1,Renaming column
df_qualifying = df_qualifying.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

# DBTITLE 1,Creating column
df_qualifying = add_date_load(df_qualifying)

# COMMAND ----------

# DBTITLE 1,write output parquet file
df_qualifying.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
