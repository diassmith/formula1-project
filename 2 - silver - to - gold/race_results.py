# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Reading drivers file and renaming some column
df_drivers = spark.read.parquet(f"{silver_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

# DBTITLE 1,Reading constructors file and renaming column
df_constructors = spark.read.parquet(f"{silver_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

# DBTITLE 1,Reading circuits file and renaming column
df_circuits = spark.read.parquet(f"{silver_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

# DBTITLE 1,Reading races file and renaming column
df_races = spark.read.parquet(f"{silver_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

# DBTITLE 1,Reading Results file and renaming column
df_results = spark.read.parquet(f"{silver_folder_path}/results") \
.withColumnRenamed("time", "race_time") 

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

display(df_races)
