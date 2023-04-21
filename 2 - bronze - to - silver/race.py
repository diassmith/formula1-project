# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with races.csv file

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing Library and Functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import to_timestamp, concat, col, lit

# COMMAND ----------

# DBTITLE 1,Reading file
# df_races = spark.read \
# .option("header", True) \
# .schema(races_schema) \
# .csv(f"{bronze_folder_path}/races.csv")


df_races = spark.read.parquet(f"{bronze_folder_path}/races")

# COMMAND ----------

# display(df_races)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_races = add_date_load_silver(df_races)\
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
#creating new column raca_timestamp, it's a column that contains the combined value between data and time column

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Selecting the all columns that I'll use.
df_races_selected = df_races.select(col('raceId').alias('race_id'), col('year').alias('race_year'),col('round'), col('circuitId').alias('circuit_id'),col('name'), col('date_load_silver'), col('race_timestamp'))

# COMMAND ----------

display(df_races_selected)

# COMMAND ----------

# DBTITLE 1,Write the output to processed container in parquet format
#df_races_selected.write.mode('overwrite').partitionBy('race_year').parquet(f"{silver_folder_path}/races")

# COMMAND ----------

# df_races_selected.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.races")

# COMMAND ----------

if spark.catalog.tableExists("f1_silver.races"):
    df_target = DeltaTable.forPath(spark, '/mnt/adlsformula1/silver/races')
    print("upsert")
    upsert(df_target,"race_id",df_races,"race_id")
else:
    print("New")
    df_races.write.mode("overwrite").format("delta").saveAsTable("f1_silver.races")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
