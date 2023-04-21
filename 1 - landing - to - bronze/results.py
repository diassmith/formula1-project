# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on results.json file

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraries and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Creating schema
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

# DBTITLE 1,Reading file
df_results = (spark.read
              .schema(results_schema)
              .json(f"{landing_folder_path}/{v_file_date}/results.json"))

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_results = (add_date_load_bronze(df_results)
              .withColumn("data_source", lit(v_data_source))
              .withColumn("file_date", lit(v_file_date)))

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Write output parquet file with partition by race_id
# df_results.write.mode("overwrite").partitionBy('raceid').parquet(f"{bronze_folder_path}/results")

# COMMAND ----------

# DBTITLE 1,Create table results
#df_results.write.mode("append").partitionBy('raceid').format("parquet").saveAsTable("f1_bronze.results")

# COMMAND ----------

if spark.catalog.tableExists("f1_bronze.results"):
    df_target = DeltaTable.forPath(spark, "/mnt/adlsformula1/bronze/results")
    print("upsert")
    upsert(df_target,"resultId",df_results,"resultId")
else:
    print("New")
    df_results.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.results")

# COMMAND ----------

# %sql
# --DROP TABLE f1_bronze.results
# --REFRESH TABLE f1_bronze.results

# COMMAND ----------

# def re_arrange_partition_column(df_input, partition_column):
#     column_list = []
#     for column_name in df_input.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)
#     df_output = df_input.select(column_list)
#     return df_output

# COMMAND ----------

# df_results = re_arrange_partition_column(df_results, 'raceId')

# COMMAND ----------

# def overwrite_partition(df_input, db_name, table_name, partition_column):
#     df_output = re_arrange_partition_column(df_input, partition_column)
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#     if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
#         df_output.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         df_output.write.mode("overwrite").partitionBy(partition_column).format(
#             "parquet"
#         ).saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# overwrite_partition(df_results, 'f1_bronze', 'results','raceId')

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# df_results = df_results.select("resultId","raceId", "driverId", "constructorId", "number", "grid", "position", "positionText", "positionOrder", "points", "laps", "time"
#                                ,"milliseconds", "fastestLap", "rank","fastestLapTime", "fastestLapSpeed", "statusId", "date_load_bronze","data_source","file_date")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_bronze.results")):
#     df_results.write.mode("overwrite").insertInto("f1_bronze.results")
# else:
#     df_results.write.mode("overwrite").partitionBy('raceid').format("parquet").saveAsTable("f1_bronze.results")
    

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# SELECT COUNT(1)
#   FROM f1_bronze.results;

# COMMAND ----------

# %sql
# SELECT raceid,  COUNT(1) 
# FROM f1_bronze.results
# GROUP BY raceid

# COMMAND ----------

# %sql
# SELECT raceid, driverid, COUNT(1) 
# FROM f1_bronze.results
# GROUP BY raceid, driverid
# HAVING COUNT(1) > 1
# --ORDER BY raceid, driverid DESC;
