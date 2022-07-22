# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading the CSV file using the spark dataframe reader

# COMMAND ----------

# DBTITLE 1,Importing Library
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

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
df_circuits = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("/mnt/adlsformula1/raw/circuits.csv")

# COMMAND ----------

df_circuits.printSchema()
