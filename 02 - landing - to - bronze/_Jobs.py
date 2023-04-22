# Databricks notebook source
v_result = dbutils.notebook.run("circuits", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18" })

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("constructors", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("drivers", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18" })

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("laptimes", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("pitstops", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("qualifying", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("race", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("results", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# v_result = dbutils.notebook.run("formula1-project/2 - bronze - to - silver/_Jobs", 0)
# 2 - bronze - to - silver/_Jobs

# COMMAND ----------

# %sql
# SELECT raceId, count(*)
# FROM f1_bronze.results
# GROUP BY raceId
# ORDER BY raceId
