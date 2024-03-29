# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with circuits.csv file

# COMMAND ----------

# DBTITLE 1,Creating Parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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
df_circuits = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{landing_folder_path}/circuits.csv")

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

# DBTITLE 1,Selected the columns that I'm going to update the name
df_circuits_selected = df_circuits.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

df_circuits_selected.show()

# COMMAND ----------

# DBTITLE 1,Renaming the columns
df_circuits_selected = df_circuits_selected.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

df_circuits_selected.show()

# COMMAND ----------

# DBTITLE 1,Creating new column to store the data load
df_circuits_selected = add_date_load(df_circuits_selected)

# COMMAND ----------

display(df_circuits_selected)

# COMMAND ----------

# DBTITLE 1,Write output parquet file
#df_circuits_selected.write.mode("overwrite").parquet(f"{bronze_folder_path}/circuits")

# COMMAND ----------

df_circuits_selected.write.mode("overwrite").format("parquet").saveAsTable("f1_bronze.circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
