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

display(df_constructors.select())

# COMMAND ----------

# DBTITLE 1,Drop the URL column
df_constructors = df_constructors.drop(col('url'))

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Renaming column and creating column
df_constructors = df_constructors .withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

# DBTITLE 1,Creating new column
df_constructors = add_date_load_silver(df_constructors)

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Write output to parquet file
#df_constructors.write.mode("overwrite").parquet(f"{silver_folder_path}/constructors")

# COMMAND ----------

# df_constructors.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.constructors")

# COMMAND ----------

# %sql

# DROP TABLE f1_silver.constructors

# COMMAND ----------

if spark.catalog.tableExists("f1_silver.constructors"):
    df_target = DeltaTable.forPath(spark, f"{silver_folder_path}"+"/constructors")
    print("upsert")
    upsert(df_target,"constructor_Id",df_constructors,"constructor_Id")
else:
    print("New")
    df_constructors.write.mode("overwrite").format("delta").saveAsTable("f1_silver.constructors")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_silver.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_silver.constructors
