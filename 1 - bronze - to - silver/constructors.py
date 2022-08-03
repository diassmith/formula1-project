# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Creating schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# DBTITLE 1,Reading file
df_constructors = spark.read\
.schema(constructors_schema)\
.json(f"{bronze_folder_path}/constructors.json")

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Importing Libraries and Functions
from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Drop the URL column
df_constructors = df_constructors.drop(col('url'))

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Renaming column and creating column
df_constructors = df_constructors .withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# DBTITLE 1,Creating new column
df_constructors = add_date_load(df_constructors)

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Write output to parquet file
df_constructors.write.mode("overwrite").parquet(f"{silver_folder_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
