# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on results.json file

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Reading file
df_results = spark.table("f1_bronze.results")

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Renaming the column and creating new column
df_results = (df_results.withColumnRenamed("driver_driverId", "driver_ref")
                        .withColumnRenamed("constructor_constructorId", "constructor_Ref")
                        .withColumnRenamed("positionText", "position_text")
                        .withColumnRenamed("positionOrder", "position_order")
                        .withColumnRenamed("fastestLap", "fastest_lap")
                        .withColumnRenamed("fastestLapTime", "fastest_lap_time")
                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed"))

# COMMAND ----------

# Adding rank column
window_spec = Window.partitionBy('race_id').orderBy('position')
df_results = df_results.withColumn('rank', F.dense_rank().over(window_spec))

# COMMAND ----------

if spark.catalog.tableExists("f1_silver.results"):
    df_target = DeltaTable.forPath(spark, '/mnt/adlsformula1/silver/results')
    print("upsert")
    upsert(df_target,"result_id",df_results,"result_id")
else:
    print("New")
    df_results.write.mode("overwrite").format("delta").saveAsTable("f1_silver.results")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
