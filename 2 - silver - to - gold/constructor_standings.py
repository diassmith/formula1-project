# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

df_race_results = spark.read.parquet(f"{gold_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

df_constructor_standings = df_race_results \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(df_constructor_standings)

# COMMAND ----------

display(df_constructor_standings.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

df_constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
df_final = df_constructor_standings.withColumn("rank", rank().over(df_constructor_rank_spec))

# COMMAND ----------

display(df_final)

# COMMAND ----------

display(df_final.filter("race_year = 2020"))

# COMMAND ----------

# DBTITLE 1,Write output parquet file in gold
#df_final.write.mode("overwrite").parquet(f"{gold_folder_path}/constructor_standings")

# COMMAND ----------

df_final.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.constructor_standings")
