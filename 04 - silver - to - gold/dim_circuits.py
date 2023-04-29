# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Dimension Circuit

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook 
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading the CSV file using the spark dataframe reader

# COMMAND ----------

# DBTITLE 1,Importing Library
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

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
                                            .withColumnRenamed("circuit_ref", "CircuitReference")
                                            .withColumnRenamed("circuit_name", "CircuitName")
                                            .withColumnRenamed("location", "Location")
                                            .withColumnRenamed("country", "Country")
                                            .withColumnRenamed("latitude", "Latitude")
                                            .withColumnRenamed("longitude", "Longitude"))

# COMMAND ----------

df_circuits = df_circuits.drop('date_load_bronze').drop('date_load_silver')

# COMMAND ----------

# df_circuits.show()

# COMMAND ----------

display(df_circuits)

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
