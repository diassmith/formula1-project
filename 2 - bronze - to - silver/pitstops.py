# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on pit_stops.json file

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraies and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

# DBTITLE 1,Reading file
# df_pit_stops = spark.read \
# .schema(pit_stops_schema) \
# .option("multiLine", True) \
# .json(f"{landing_folder_path}/pit_stops.json")

df_pit_stops = spark.read.parquet(f"{bronze_folder_path}/pit_stops")

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Renaming columns and creating new column
df_pit_stops = df_pit_stops.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

# DBTITLE 1,Creating new column
df_pit_stops = add_date_load_silver(df_pit_stops)

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
#df_pit_stops.write.mode("overwrite").parquet(f"{silver_folder_path}/pit_stops")

# COMMAND ----------

df_pit_stops.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
