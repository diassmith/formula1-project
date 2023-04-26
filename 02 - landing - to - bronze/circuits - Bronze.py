# Databricks notebook source
# MAGIC %md
# MAGIC ### Working Circuits Bronze Layer

# COMMAND ----------

# DBTITLE 1,Importing Library
from delta.tables import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Creating Parameters
# dbutils.widgets.text("p_data_source", "")
# v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# dbutils.widgets.text("p_file_date", "2021-03-21")
# v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook 
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

df_circuits = spark.read.parquet(f"{landing_folder_path}/circuits")

# COMMAND ----------

df_circuits = (add_date_load_bronze(df_circuits))

# COMMAND ----------

# for column in df_circuits.columns:
#     print("'"+column+"'"+",")

# COMMAND ----------

df_circuits = df_circuits.select('locality','country',
'lat','long','circuitId','circuitName','url','date_ref','date_load_bronze','SkCircuits')

# COMMAND ----------

if spark.catalog.tableExists("f1_bronze.circuits"):
    df_target = DeltaTable.forPath(spark, f"{bronze_folder_path}"+"/circuits")
    print("upsert")
    upsert(df_target,"SkCircuits",df_circuits,"SkCircuits")
else:
    print("New")
    df_circuits.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.circuits
