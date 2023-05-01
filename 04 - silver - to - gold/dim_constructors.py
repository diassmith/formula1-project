# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Dimension Constructors

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook 
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Reading Delta Table
df_constructors = spark.table("f1_silver.constructors")

# COMMAND ----------

# df_constructors.printSchema()

# COMMAND ----------

# DBTITLE 1,Renaming the columns
df_constructors = (df_constructors.withColumnRenamed("constructorId", "ConstructorReference")
                                            .withColumnRenamed("id", "ConstructorId")
                                            .withColumnRenamed("name", "Name")
                                            .withColumnRenamed("nationality", "Nationality"))

# COMMAND ----------

df_constructors = df_constructors.drop('date_load_bronze').drop('date_load_silver')

# COMMAND ----------

# display(df_constructors)

# COMMAND ----------

# DBTITLE 1,Create dim_Circuits
if spark.catalog.tableExists("f1_gold.dim_Constructors"):
    df_target = DeltaTable.forPath(spark, f"{gold_folder_path}"+"/dim_Constructors")
    print("upsert")
    upsert(df_target,"ConstructorId",df_constructors,"ConstructorId")
else:
    print("New")
    df_constructors.write.mode("overwrite").format("delta").saveAsTable("f1_gold.dim_Constructors")

# COMMAND ----------

dbutils.notebook.exit("Sucess")

# COMMAND ----------

# %sql
# SELECT * FROM f1_gold.dim_Constructors
