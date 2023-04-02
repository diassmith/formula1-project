# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Reading drivers file and renaming some column
df_drivers = spark.read.parquet(f"{silver_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# DBTITLE 1,Reading constructors file and renaming column
df_constructors = spark.read.parquet(f"{silver_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Reading circuits file and renaming column
df_circuits = spark.read.parquet(f"{silver_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

# DBTITLE 1,Reading races file and renaming column
df_races = spark.read.parquet(f"{silver_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Reading Results file and renaming column
df_results = spark.read.parquet(f"{silver_folder_path}/results") \
.withColumnRenamed("time", "race_time") 

# COMMAND ----------

display(df_results)

# COMMAND ----------

# DBTITLE 1,Join Race and Circuits
df_races_circuits = df_races.join(df_circuits, df_races.circuit_id == df_circuits.circuit_id, "inner") \
.select(df_races.race_id, df_races.race_year, df_races.race_name, df_races.race_date, df_circuits.circuit_location)

# COMMAND ----------

display(df_races_circuits)

# COMMAND ----------

# DBTITLE 1,Join Results to all dataframes 
df_race_results = df_results.join(df_races_circuits, df_results.race_id == df_races_circuits.race_id) \
                            .join(df_drivers, df_results.driver_id == df_drivers.driver_id) \
                            .join(df_constructors, df_results.constructor_id == df_constructors.constructor_id)

# COMMAND ----------

df_final = df_race_results.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position")\
                                .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(df_final)

# COMMAND ----------

# DBTITLE 1,Creating table igual BBC website
display(df_final.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(df_final.points.desc()))

# COMMAND ----------

# DBTITLE 1,Write output in Gold Layer
#df_final.write.mode("overwrite").parquet(f"{gold_folder_path}/race_results")

# COMMAND ----------

df_final.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.race_results")
