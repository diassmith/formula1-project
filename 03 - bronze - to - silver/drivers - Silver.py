# Databricks notebook source
# MAGIC %md
# MAGIC ### Working drivers file

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing Libraries and Functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

# # df_drivers = spark.read\
# # .schema(drivers_schema)\
# # .json(f"{landing_folder_path}/drivers.json")

# df_drivers = spark.read.parquet(f"{bronze_folder_path}/drivers")

# COMMAND ----------

df_drivers = spark.table("f1_bronze.drivers")

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Droping URL column
df_drivers = df_drivers.drop(col("url"))

# COMMAND ----------

# DBTITLE 1,Renaming  the columns
df_drivers = (df_drivers.withColumnRenamed("driverId", "driverRef")
                        .withColumnRenamed("givenName", "forename")
                        .withColumnRenamed("familyName", "surname")
                        .withColumnRenamed("permanentNumber", "number")
                        .withColumn("fullName", concat(col("forename"), lit(" "), col("surname")))
                        .select('id','code','dateOfBirth','driverRef','surname','forename','fullName','nationality','number','year','date_load_bronze'))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_drivers = add_date_load_silver(df_drivers)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Write output on parquet file in processed layer
if spark.catalog.tableExists("f1_silver.drivers"):
    df_target = DeltaTable.forPath(spark, f"{silver_folder_path}"+"/drivers")
    print("upsert")
    upsert(df_target,"id",df_drivers,"id")
else:
    print("New")
    df_drivers.write.mode("overwrite").format("delta").saveAsTable("f1_silver.drivers")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
