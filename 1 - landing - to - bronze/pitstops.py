# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on pit_stops.json file

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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

# DBTITLE 1,Creating Schema
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# DBTITLE 1,Reading file
df_pit_stops = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{landing_folder_path}/pit_stops.json")

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_pit_stops = add_date_load_bronze(df_pit_stops)\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
#df_pit_stops.write.mode("overwrite").parquet(f"{bronze_folder_path}/pit_stops")

# COMMAND ----------

df_pit_stops.write.mode("overwrite").format("parquet").saveAsTable("f1_bronze.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
