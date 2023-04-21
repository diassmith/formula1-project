# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on lap_times files

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

# DBTITLE 1,Reading folder
# df_lap_times = spark.read \
# .schema(lap_times_schema) \
# .csv(f"{landing_folder_path}/lap_times")

df_lap_times = spark.read.parquet(f"{bronze_folder_path}/lap_times")

# COMMAND ----------

# DBTITLE 1,Renaming column and creating new column
df_lap_times = df_lap_times.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

# DBTITLE 1,Creating new column
df_lap_times = add_date_load_silver(df_lap_times)

# COMMAND ----------

display(df_lap_times)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
#df_lap_times.write.mode("overwrite").parquet(f"{silver_folder_path}/lap_times")

# COMMAND ----------

# df_lap_times.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.lap_times")

# COMMAND ----------

if spark.catalog.tableExists("f1_silver.lap_times"):
    df_target = DeltaTable.forPath(spark, f"{silver_folder_path}"+"/lap_times")
    print("upsert")
    upsert2(df_target,"raceId","driver_id",df_lap_times,"raceId","driver_id")
else:
    print("New")
    df_lap_times.write.mode("overwrite").format("delta").saveAsTable("f1_silver.lap_times")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
