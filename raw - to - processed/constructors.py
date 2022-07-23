# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# DBTITLE 1,Creating schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# DBTITLE 1,Reading file
df_constructors = spark.read\
.schema(constructors_schema)\
.json("/mnt/adlsformula1/raw/constructors.json")

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Importing Libraries and Functions
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Drop the URL column
df_constructors = df_constructors.drop(col('url'))

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Renaming column and creating data_load column
df_constructors = df_constructors .withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")\
.withColumn("data_load", current_timestamp())

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Write output to parquet file
df_constructors.write.mode("overwrite").parquet("/mnt/adlsformula1/processed/constructors")
