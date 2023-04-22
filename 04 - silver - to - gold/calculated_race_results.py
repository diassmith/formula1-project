# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE f1_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables f1_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_gold.calculated_race_results
# MAGIC USING parquet 
# MAGIC AS
# MAGIC SELECT races.race_year
# MAGIC         ,constructors.name as team_name
# MAGIC         ,drivers.name as driver_name
# MAGIC         ,results.position
# MAGIC         ,results.points
# MAGIC         , 11 - results.position as calculated_points
# MAGIC   FROM f1_silver.results
# MAGIC   JOIN f1_silver.drivers ON (results.driver_id = drivers.driver_id)
# MAGIC   JOIN f1_silver.constructors ON (results.constructor_id = constructors.constructor_id)
# MAGIC   JOIN f1_silver.races ON (results.race_id = races.race_id)
# MAGIC   where f1_silver.results.position <= 10

# COMMAND ----------


