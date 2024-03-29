# Databricks notebook source
# MAGIC %md
# MAGIC ### Circuits Bronze

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook 
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

df_circuits = spark.read.parquet(f"{landing_folder_path}/circuits")

# COMMAND ----------

df_circuits = (add_date_load_bronze(df_circuits))

# COMMAND ----------

df_circuits = df_circuits.select('locality','country',
'lat','long','circuitId','circuitName','url','id','date_ref','date_load_bronze')

# COMMAND ----------

# DBTITLE 1,Creating the Circuits bronze
if spark.catalog.tableExists("f1_bronze.circuits"):
    df_target = DeltaTable.forPath(spark, f"{bronze_folder_path}"+"/circuits")
    print("upsert")
    upsert(df_target,"id",df_circuits,"id")
else:
    print("New")
    df_circuits.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# SELECT * FROM f1_bronze.circuits
