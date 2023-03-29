# Databricks notebook source
# MAGIC %md
# MAGIC ### Working drivers file

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

# DBTITLE 1,Importing Libraries and Functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

# DBTITLE 1,Creating schema for name colmun
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# COMMAND ----------

# DBTITLE 1,Creating schema
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

df_drivers = spark.read\
.schema(drivers_schema)\
.json(f"{landing_folder_path}/drivers.json")

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_drivers = add_date_load_bronze(df_drivers)\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Write output on parquet file in processed layer
#df_drivers.write.mode("overwrite").parquet(f"{bronze_folder_path}/drivers")

# COMMAND ----------

df_drivers.write.mode("overwrite").format("parquet").saveAsTable("f1_bronze.drivers")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
