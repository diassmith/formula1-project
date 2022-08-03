# Databricks notebook source
# MAGIC %md
# MAGIC ### Working on qualifying json files

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing libraries and functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

# DBTITLE 1,Creating schema
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

# DBTITLE 1,Reading folder
df_qualifying = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{bronze_folder_path}/qualifying")

# COMMAND ----------

# DBTITLE 1,Renaming column and creating new column 
df_qualifying = df_qualifying.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# DBTITLE 1,Creating column
df_qualifying = add_date_load(df_qualifying)

# COMMAND ----------

# DBTITLE 1,write output parquet file
df_qualifying.write.mode("overwrite").parquet(f"{silver_folder_path}/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
