# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with circuits.csv file

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
df_circuits = spark.read.parquet(f"{bronze_folder_path}/circuits")

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

# DBTITLE 1,Selected the columns
df_circuits_selected = df_circuits.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# df_circuits_selected.show()

# COMMAND ----------

# DBTITLE 1,Renaming the columns
df_circuits_selected = df_circuits_selected.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

df_circuits_selected.show()

# COMMAND ----------

# DBTITLE 1,Creating new column to store the data load
df_circuits_selected = add_date_load_silver(df_circuits_selected)

# COMMAND ----------

display(df_circuits_selected)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
#df_circuits_selected.write.mode("overwrite").parquet(f"{silver_folder_path}/circuits")

# COMMAND ----------

df_circuits_selected.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
