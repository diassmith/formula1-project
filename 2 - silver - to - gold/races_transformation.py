# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Reading file from silver layer
df_races = spark.read.parquet(f"{silver_folder_path}/races")

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Filtering data
#df_races = df_races.filter("race_year = 2019 and round <= 5")

df_races = df_races.where((df_races["race_year"] == 2019) & (df_races["round"] <= 5))

# COMMAND ----------

display(df_races)
