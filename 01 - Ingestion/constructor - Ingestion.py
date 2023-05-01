# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

response = requests.get('https://ergast.com/api/f1/constructors.json?limit=2023')
json_data = response.json()

constructors = json_data['MRData']['ConstructorTable']['Constructors']
df_constructors = spark.createDataFrame(constructors)

# COMMAND ----------

df_constructors = (df_constructors.withColumn('id', abs(hash(concat("constructorId", df_constructors["constructorId"])))))

# COMMAND ----------

df_constructors = add_date_load_landing(df_constructors)

# COMMAND ----------

# display(df_constructors)

# COMMAND ----------

df_constructors.write.mode("overwrite").parquet(f"{landing_folder_path}/constructors")
