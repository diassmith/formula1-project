# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_bronze
# MAGIC LOCATION "/mnt/adlsformula1/bronze"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE f1_bronze;

# COMMAND ----------

# %sql
# DROP SCHEMA IF EXISTS f1_bronze CASCADE
