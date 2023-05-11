# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on rresults bronze

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

df_results = spark.read.parquet(f"{landing_folder_path}/results")

# COMMAND ----------

# display(df_results)

# COMMAND ----------

# DBTITLE 1,Update
if spark.catalog.tableExists("f1_bronze.results"):
    df_target = DeltaTable.forPath(spark, "/mnt/adlsformula1/bronze/results")
    print("upsert")
    upsert(df_target,"resultId",df_results,"resultId")
else:
    print("New")
    df_results.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.results")

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
