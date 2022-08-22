# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

df_race_results = spark.read.parquet(f"{gold_folder_path}/race_results")

# COMMAND ----------

display(df_race_results)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

df_driver_standings = df_race_results \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(df_driver_standings)

# COMMAND ----------

display(df_driver_standings.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

df_driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
df_final = df_driver_standings.withColumn("rank", rank().over(df_driver_rank_spec))

# COMMAND ----------

display(df_final.filter("race_year = 2020"))

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{gold_folder_path}/driver_standings")
