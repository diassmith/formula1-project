# Databricks notebook source
# MAGIC %md
# MAGIC ### Working drivers file

# COMMAND ----------

# DBTITLE 1,Importing Libraries and Functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

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
.json("/mnt/adlsformula1/raw/drivers.json")

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Renaming  the columns
df_drivers = df_drivers.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("date_load", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Droping URL column
df_drivers = df_drivers.drop(col("url"))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Write output on parquet file in processed layer
df_drivers.write.mode("overwrite").parquet("/mnt/adlsformula1/processed/drivers")
