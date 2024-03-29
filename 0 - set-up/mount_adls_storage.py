# Databricks notebook source
# DBTITLE 1,Creating variables to save connections strings
storage_account_name = "adlsformula1"
client_id       = dbutils.secrets.get(scope="formula1-scope", key="clientId-Secret")
tenant_id       = dbutils.secrets.get(scope="formula1-scope", key="tenantId-Secret")
client_secret   = dbutils.secrets.get(scope="formula1-scope", key="client-Secret")

# COMMAND ----------

# DBTITLE 1,Doing connection
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# DBTITLE 1,Creating the functions taking the container name
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,call the function to create mount
#mount_adls("landing")
# mount_adls("bronze")
# mount_adls("silver")
# mount_adls("gold")

# COMMAND ----------

# DBTITLE 1,Checking the dbfs

#%fs ls mnt/adlsformula1/landing
 

# COMMAND ----------

#dbutils.fs.ls("/mnt/adlsformula1")

# COMMAND ----------

#dbutils.fs.rm("/mnt/adlsformula1", True)

# COMMAND ----------

#dbutils.fs.unmount("/mnt/adlsformula1/gold")
#dbutils.fs.unmount("/mnt/adlsformula1/silver")
#dbutils.fs.unmount("/mnt/adlsformula1/bronze")
