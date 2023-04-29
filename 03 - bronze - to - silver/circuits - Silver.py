# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with circuits delta in Silver Layer

# COMMAND ----------

# DBTITLE 1,Importing Library
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook 
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading the delta table circuits from bronze

# COMMAND ----------

# DBTITLE 1,Reading the file
df_circuits = spark.table("f1_bronze.circuits")

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

df_circuits = (df_circuits.withColumnRenamed("circuitId", "circuit_ref")
                          .withColumnRenamed("locality", "location")
                          .withColumnRenamed("lat","latitude")
                          .withColumnRenamed("long", "longitude")
                          .withColumnRenamed("circuitName","circuit_name").select('id','circuit_ref','circuit_name','location','country','latitude','longitude','date_ref','date_load_bronze'))

# COMMAND ----------

df_circuits = add_date_load_silver(df_circuits)

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
if spark.catalog.tableExists("f1_silver.circuits"):
    df_target = DeltaTable.forPath(spark, f"{silver_folder_path}"+"/circuits")
    print("upsert")
    upsert(df_target,circuit_id,df_circuits,circuit_id)
else:
    print("New")
    df_circuits.write.mode("overwrite").format("delta").saveAsTable("f1_silver.circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# SELECT * FROM f1_silver.circuits
