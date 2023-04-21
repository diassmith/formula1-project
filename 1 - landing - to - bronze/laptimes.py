# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on lap_times files

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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

# DBTITLE 1,Creating schema
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# DBTITLE 1,Reading folder
df_lap_times = (spark.read
                .schema(lap_times_schema)
                .csv(f"{landing_folder_path}/{v_file_date}/lap_times"))

# COMMAND ----------

# DBTITLE 1,Creating new column
df_lap_times = (add_date_load_bronze(df_lap_times)\
                .withColumn("data_source", lit(v_data_source))\
                .withColumn("file_date", lit(v_file_date)))

# COMMAND ----------

display(df_lap_times)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
#df_lap_times.write.mode("overwrite").parquet(f"{bronze_folder_path}/lap_times")

# COMMAND ----------

# df_lap_times.write.mode("overwrite").format("parquet").saveAsTable("f1_bronze.lap_times")

# COMMAND ----------

if spark.catalog.tableExists("f1_bronze.lap_times"):
    df_target = DeltaTable.forPath(spark, "/mnt/adlsformula1/bronze/lap_times")
    print("upsert")
    upsert(df_target,"id",df_lap_times,"id")
else:
    print("New")
    df_lap_times.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.lap_times")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
