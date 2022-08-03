# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Reading and filtering the Circuits
df_circuits = spark.read.parquet(f"{silver_folder_path}/circuits") \
.filter("circuit_id < 70") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

# DBTITLE 1,Reading and filtering the Races
df_races = spark.read.parquet(f"{silver_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

display(df_races)
