# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on lap_times files

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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
df_lap_times = spark.read \
.schema(lap_times_schema) \
.csv(f"{bronze_folder_path}/lap_times")

# COMMAND ----------

# DBTITLE 1,Renaming column and creating new column
df_lap_times = df_lap_times.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# DBTITLE 1,Creating new column
df_lap_times = add_date_load(df_lap_times)

# COMMAND ----------

display(df_lap_times)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
df_lap_times.write.mode("overwrite").parquet(f"{silver_folder_path}/lap_times")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
