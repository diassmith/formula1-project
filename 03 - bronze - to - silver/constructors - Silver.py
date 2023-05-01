# Databricks notebook source
# MAGIC %md
# MAGIC ### Constructors Silver

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Reading file
df_constructors = spark.table("f1_bronze.constructors")

# COMMAND ----------

df_constructors = (df_constructors.select('id','constructorId','name','nationality','date_ref','date_load_bronze'))

# COMMAND ----------

df_constructors = add_date_load_silver(df_constructors)

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Write output to parquet file
if spark.catalog.tableExists("f1_silver.constructors"):
    df_target = DeltaTable.forPath(spark, f"{silver_folder_path}"+"/constructors")
    print("upsert")
    upsert(df_target,"id",df_constructors,"id")
else:
    print("New")
    df_constructors.write.mode("overwrite").format("delta").saveAsTable("f1_silver.constructors")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql

# SELECT * FROM f1_silver.constructors
