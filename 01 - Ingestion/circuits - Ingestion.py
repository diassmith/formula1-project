# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

response = requests.get('https://ergast.com/api/f1/circuits.json')
json_data = response.json()

df_circuits = spark.createDataFrame(json_data['MRData']['CircuitTable']['Circuits'])

# COMMAND ----------

df_circuits = df_circuits.select(col('Location').getItem('locality').alias('locality'),
               col('Location').getItem('country').alias('country'),
               col('Location').getItem('lat').alias('lat'), 
               col('Location').getItem('long').alias('long'),
               col('circuitId'), 
               col('circuitName'), 
               col('url'))

# COMMAND ----------

df_circuits = (add_date_load_landing(df_circuits))

# COMMAND ----------

df_circuits = (df_circuits.orderBy(asc("locality"))
                          .withColumn('id', abs(hash(concat("circuitId", df_circuits["circuitId"])))))

# COMMAND ----------

df_circuits.write.mode("overwrite").parquet(f"{landing_folder_path}/circuits")
