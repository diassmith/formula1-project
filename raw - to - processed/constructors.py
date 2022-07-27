# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../includes/functions"

# COMMAND ----------

# DBTITLE 1,Creating schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# DBTITLE 1,Reading file
df_constructors = spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Importing Libraries and Functions
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Drop the URL column
df_constructors = df_constructors.drop(col('url'))

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Renaming column and creating data_load column
df_constructors = df_constructors .withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

# DBTITLE 1,Creating new column
df_constructors = add_date_load(df_constructors)

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Write output to parquet file
df_constructors.write.mode("overwrite").parquet("f"{processed_folder_path}/constructors")
