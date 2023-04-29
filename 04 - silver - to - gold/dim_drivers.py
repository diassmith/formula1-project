# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Dimension Drivers

# COMMAND ----------

# DBTITLE 1,Importing Library
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
from pyspark.sql.window import Window
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

# DBTITLE 1,Reading the file
df_drivers = spark.table("f1_silver.drivers")

# COMMAND ----------

df_drivers.printSchema()

# COMMAND ----------

# DBTITLE 1,Renaming the columns
df_drivers = (df_drivers.withColumnRenamed("id", "DriverId")
                        .withColumnRenamed("driverRef", "Driver")
                        .withColumnRenamed("surname", "Surname")
                        .withColumnRenamed("forename", "Forename")
                        .withColumnRenamed("fullName", "FullName")
                        .withColumnRenamed("nationality", "Nationality")
                        .withColumnRenamed("number", "Number"))

# COMMAND ----------

# DBTITLE 1,Creating column FirstYear
#This is column has been created to storage the year drivers first race
window = Window.partitionBy('DriverId')

df_drivers = df_drivers.withColumn('FirstRace', F.min('year').over(window))

# COMMAND ----------

# DBTITLE 1,Group by drivers
window = Window.partitionBy("DriverId").orderBy(F.col("year").desc())

df_ranked = df_drivers.withColumn("row_number", F.row_number().over(window))

df_drivers = df_ranked.filter(F.col("row_number") == 1)

df_drivers = df_drivers.drop("row_number")

# COMMAND ----------

df_drivers = df_drivers.drop('date_load_bronze').drop('date_load_silver')

# COMMAND ----------

# display(df_drivers.filter("Surname = 'Hamilton'"))

# COMMAND ----------

# DBTITLE 1,Create dim_Drivers
if spark.catalog.tableExists("f1_gold.dim_Drivers"):
    df_target = DeltaTable.forPath(spark, f"{gold_folder_path}"+"/dim_Drivers")
    print("upsert")
    upsert(df_target,"DriverId",df_drivers,"DriverId")
else:
    print("New")
    df_drivers.write.mode("overwrite").format("delta").saveAsTable("f1_gold.dim_Drivers")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# SELECT * FROM f1_gold.dim_Drivers
