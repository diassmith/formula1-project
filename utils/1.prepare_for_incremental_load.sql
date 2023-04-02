-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_bronze CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_bronze
LOCATION "/mnt/adlsformula1/bronze";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_silver CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_silver
LOCATION "/mnt/adlsformula1/silver";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_gold CASCADE;

-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS f1_gold 
LOCATION "/mnt/adlsformula1/gold";
