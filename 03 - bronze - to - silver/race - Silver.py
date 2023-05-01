# Databricks notebook source
# MAGIC %md
# MAGIC ### races Silver

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Reading Delta Table
df_races = spark.table("f1_bronze.races")

# COMMAND ----------

df_races = add_date_load_silver(df_races)

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
                        col('Sprint').getItem('time').alias('sprint_time'),
                        col('date_ref'),col('date_load_bronze'),col('date_load_silver')))

# COMMAND ----------

df_races = df_races.withColumnRenamed("circuitId","circuitRef")

# COMMAND ----------

df_races = df_races.withColumn('circuitId', abs(hash(df_races["circuitRef"])))

# COMMAND ----------

df_races = (df_races.select(col('raceId'),col('season'),col('round'),col('circuitId'),col('circuitRef'),col('url'),
                            col('fp1_date'),col('fp1_time'),col('fp2_date'),col('fp2_time'),col('fp3_date'),
                            col('fp3_time'),col('quali_date'),col('quali_time'),col('sprint_date'),col('sprint_time'),
                            col('date_ref'),col('date_load_bronze'),col('date_load_silver')))

# COMMAND ----------

# DBTITLE 1,Creating Races Silver
if spark.catalog.tableExists("f1_silver.races"):
    df_target = DeltaTable.forPath(spark, '/mnt/adlsformula1/silver/races')
    print("upsert")
    upsert(df_target,"raceId",df_races,"raceId")
else:
    print("New")
    df_races.write.mode("overwrite").format("delta").saveAsTable("f1_silver.races")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
