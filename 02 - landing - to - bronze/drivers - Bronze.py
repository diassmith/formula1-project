# Databricks notebook source
# MAGIC %md
# MAGIC ### Working drivers file

# COMMAND ----------

# DBTITLE 1,Importing Libraries and Functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, lit
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Creating parameters
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

df_drivers = spark.read.parquet(f"{landing_folder_path}/drivers")

# COMMAND ----------

df_drivers = (add_date_load_bronze(df_drivers))

# COMMAND ----------

df_drivers = df_drivers.select('id','code','dateOfBirth','driverId','familyName','givenName','nationality','permanentNumber','url','year','date_load_bronze')

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

if spark.catalog.tableExists("f1_bronze.drivers"):
    df_target = DeltaTable.forPath(spark, f"{bronze_folder_path}"+"/drivers")
    print("upsert")
    upsert(df_target,"id",df_drivers,"id")
else:
    print("New")
    df_drivers.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.drivers")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
