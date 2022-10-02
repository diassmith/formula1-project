# Databricks notebook source
# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw

# COMMAND ----------

# DBTITLE 1,Show databases
# MAGIC %sql
# MAGIC SHOW databases

# COMMAND ----------

# MAGIC %sql
# MAGIC Use f1_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE f1_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select CURRENT_DATABASE()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.circuits(circuitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country STRING,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/adlsformula1/bronze/circuits.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_bronze.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.races(raceId INT,
# MAGIC year INT,
# MAGIC round INT,
# MAGIC circuitId INT,
# MAGIC name STRING,
# MAGIC date DATE,
# MAGIC time STRING,
# MAGIC url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/adlsformula1/bronze/races.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.races
