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

df_races = (df_races.select(col('raceId'),col('season'),col('round'),col('Circuit').getItem('circuitId').alias('circuitId'),
                        col('raceName'),col('date'),col('time'),col('url'),
                        col('FirstPractice').getItem('date').alias('fp1_date'),
                        col('FirstPractice').getItem('time').alias('fp1_time'),
                        col('SecondPractice').getItem('date').alias('fp2_date'),
                        col('SecondPractice').getItem('time').alias('fp2_time'),
                        col('ThirdPractice').getItem('date').alias('fp3_date'),
                        col('ThirdPractice').getItem('time').alias('fp3_time'),
                        col('Qualifying').getItem('date').alias('quali_date'),
                        col('Qualifying').getItem('time').alias('quali_time'),
                        col('Sprint').getItem('date').alias('sprint_date'),
                        col('Sprint').getItem('time').alias('sprint_time')))

# COMMAND ----------

df_races.write.mode("overwrite").parquet(f"{landing_folder_path}/races")
