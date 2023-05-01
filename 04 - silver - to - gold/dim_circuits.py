# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Dimension Circuit

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook 
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Reading the file
df_circuits = spark.table("f1_silver.circuits")

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

# display(df_circuits)

# COMMAND ----------

# DBTITLE 1,Renaming the columns
df_circuits = (df_circuits.withColumnRenamed("id", "CircuitId")
                                            .withColumnRenamed("circuitRef", "CircuitReference")
                                            .withColumnRenamed("circuit_name", "CircuitName")
                                            .withColumnRenamed("location", "Location")
                                            .withColumnRenamed("country", "Country")
                                            .withColumnRenamed("latitude", "Latitude")
                                            .withColumnRenamed("longitude", "Longitude"))

# COMMAND ----------

df_circuits = df_circuits.drop('date_load_bronze').drop('date_load_silver')

# COMMAND ----------

# display(df_circuits)

# COMMAND ----------

# DBTITLE 1,Create dim_Circuits
if spark.catalog.tableExists("f1_gold.dim_Circuits"):
    df_target = DeltaTable.forPath(spark, f"{gold_folder_path}"+"/dim_Circuits")
    print("upsert")
    upsert(df_target,"CircuitId",df_circuits,"CircuitId")
else:
    print("New")
    df_circuits.write.mode("overwrite").format("delta").saveAsTable("f1_gold.dim_Circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# SELECT * FROM f1_gold.dim_circuits
