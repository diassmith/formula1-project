# Databricks notebook source
# import requests
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

# COMMAND ----------

# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# Get year 
date_time_utc = datetime.utcnow().year

# COMMAND ----------

response = requests.get('https://ergast.com/api/f1/'+str(date_time_utc)+'.json')
json_data = response.json()

df_races= spark.createDataFrame(json_data['MRData']['RaceTable']['Races'])

# COMMAND ----------

df_races = df_races.withColumn('raceId', abs(hash( df_races["raceName"])))

# COMMAND ----------

df_races = add_date_load_landing(df_races)

# COMMAND ----------

df_races.write.mode("overwrite").parquet(f"{landing_folder_path}/races")
