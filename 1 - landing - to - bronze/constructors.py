# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Creating schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# DBTITLE 1,Reading file
df_constructors = (spark.read
                   .schema(constructors_schema)
                   .json(f"{landing_folder_path}/{v_file_date}/constructors.json"))

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_constructors = (add_date_load_bronze(df_constructors)
                   .withColumn("data_source", lit(v_data_source))
                   .withColumn("file_date", lit(v_file_date)))

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Write output to parquet file
#df_constructors.write.mode("overwrite").parquet(f"{bronze_folder_path}/constructors")

# COMMAND ----------

# df_constructors.write.mode("overwrite").format("parquet").saveAsTable("f1_bronze.constructors")

# COMMAND ----------

if spark.catalog.tableExists("f1_bronze.constructors"):
    df_target = DeltaTable.forPath(spark, f"{bronze_folder_path}"+"/constructors")
    print("upsert")
    upsert(df_target,"constructorId",df_constructors,"constructorId")
else:
    print("New")
    df_constructors.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.constructors")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# Drop table f1_bronze.constructors
