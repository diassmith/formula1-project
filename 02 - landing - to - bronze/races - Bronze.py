# Databricks notebook source
# MAGIC %md
# MAGIC ### races Bronze

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

df_races = spark.read.parquet(f"{landing_folder_path}/races")

# COMMAND ----------

df_races = add_date_load_bronze(df_races)

# COMMAND ----------

# DBTITLE 1,Write the output to processed container in parquet format
if spark.catalog.tableExists("f1_bronze.races"):
    df_target = DeltaTable.forPath(spark, "/mnt/adlsformula1/bronze/races")
    print("upsert")
    upsert(df_target,"raceId",df_races,"raceId")
else:
    print("New")
    df_races.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.races")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
