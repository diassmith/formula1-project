# Databricks notebook source
storage_account_name = "adlsformula1"
client_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-secret")
