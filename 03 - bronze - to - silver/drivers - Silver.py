# Databricks notebook source
# MAGIC %md
# MAGIC ### Drivers Silver

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

df_drivers = spark.table("f1_bronze.drivers")

# COMMAND ----------

# DBTITLE 1,Droping URL column
df_drivers = df_drivers.drop(col("url"))

# COMMAND ----------

# DBTITLE 1,Renaming  the columns
df_drivers = (df_drivers.withColumnRenamed("driverId", "driverRef")
                        .withColumnRenamed("givenName", "forename")
                        .withColumnRenamed("familyName", "surname")
                        .withColumnRenamed("permanentNumber", "number")
                        .withColumn("fullName", concat(col("forename"), lit(" "), col("surname")))
                        .select('id','code','dateOfBirth','driverRef','surname','forename','fullName','nationality','number','year','date_load_bronze'))

# COMMAND ----------

# DBTITLE 1,Creating new column
df_drivers = add_date_load_silver(df_drivers)

# COMMAND ----------

# DBTITLE 1,Creating Drivers Delta Table 
if spark.catalog.tableExists("f1_silver.drivers"):
    df_target = DeltaTable.forPath(spark, f"{silver_folder_path}"+"/drivers")
    print("upsert")
    upsert(df_target,"id",df_drivers,"id")
else:
    print("New")
    df_drivers.write.mode("overwrite").format("delta").saveAsTable("f1_silver.drivers")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
