# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with circuits.csv file

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Creating Parameters
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

# MAGIC %md
# MAGIC ##### Reading the CSV file using the spark dataframe reader

# COMMAND ----------

# DBTITLE 1,Importing Library
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Creating the new schema
circuits_schema = StructType(fields =[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Reading the file
df_circuits = (spark.read
               .option("header", True)
               .schema(circuits_schema)
               .csv(f"{landing_folder_path}/{v_file_date}/circuits.csv"))

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

# DBTITLE 1,Creating new column to store the data load
df_circuits = (add_date_load_bronze(df_circuits)
               .withColumn("data_source", lit(v_data_source))
               .withColumn("file_date", lit(v_file_date)))

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
#df_circuits_selected.write.mode("overwrite").parquet(f"{bronze_folder_path}/circuits")

# COMMAND ----------

if spark.catalog.tableExists("f1_bronze.circuits"):
    df_target = DeltaTable.forPath(spark, "/mnt/adlsformula1/bronze/circuits")
    print("upsert")
    upsert(df_target,"circuitId",df_circuits,"circuitId")
else:
    print("New")
    df_circuits.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.circuits")

# COMMAND ----------

#df_circuits.write.mode("overwrite").format("delta").saveAsTable("f1_bronze.circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.circuits
