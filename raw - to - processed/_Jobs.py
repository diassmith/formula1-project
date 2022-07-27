# Databricks notebook source
v_result = dbutils.notebook.run("circuits", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("constructors", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
