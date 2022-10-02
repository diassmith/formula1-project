# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_silver
# MAGIC LOCATION "/mnt/adlsformula1/silver"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE f1_silver;
