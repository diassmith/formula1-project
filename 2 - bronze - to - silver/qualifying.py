# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on qualifying json files

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraries and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

# DBTITLE 1,Reading folder
# df_qualifying = spark.read\
# .schema(qualifying_schema)\
# .option("multiLine", True)\
# .json(f"{landing_folder_path}/qualifying")

df_qualifying = spark.read.parquet(f"{bronze_folder_path}/qualifying")

# COMMAND ----------

# DBTITLE 1,Renaming column and creating new column 
df_qualifying = df_qualifying.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

# DBTITLE 1,Creating column
df_qualifying = add_date_load_silver(df_qualifying)

# COMMAND ----------

# DBTITLE 1,write output parquet file
#df_qualifying.write.mode("overwrite").parquet(f"{silver_folder_path}/qualifying")

# COMMAND ----------

# df_qualifying.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.qualifying")

# COMMAND ----------

if spark.catalog.tableExists("f1_silver.qualifying"):
    df_target = DeltaTable.forPath(spark, '/mnt/adlsformula1/silver/qualifying')
    print("upsert")
    upsert(df_target,"qualify_id",df_qualifying,"qualify_id")
else:
    print("New")
    df_qualifying.write.mode("overwrite").format("delta").saveAsTable("f1_silver.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
