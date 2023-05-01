# Databricks notebook source
# MAGIC %md
# MAGIC ### Constructors Bronze

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

df_constructors = spark.read.parquet(f"{landing_folder_path}/constructors")

# COMMAND ----------

df_constructors = add_date_load_bronze(df_constructors)

# COMMAND ----------

df_constructors.display()

# COMMAND ----------

# DBTITLE 1,Write output to parquet file
if spark.catalog.tableExists("f1_bronze.constructors"):
    df_target = DeltaTable.forPath(spark, f"{bronze_folder_path}"+"/constructors")
    print("upsert")
    upsert(df_target,"id",df_constructors,"id")
else:
    print("New")
    df_constructors.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.constructors")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# SELECT * FROM f1_bronze.constructors
