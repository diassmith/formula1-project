# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with races.csv file

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Run the configuration notebook
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run the functions notebook 
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# DBTITLE 1,Importing Library and Functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import to_timestamp, concat, col, lit

# COMMAND ----------

# DBTITLE 1,Creating Schema
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

# DBTITLE 1,Reading file
df_races = (spark.read
            .option("header", True)
            .schema(races_schema)
            .csv(f"{landing_folder_path}/{v_file_date}/races.csv"))

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_races = (add_date_load_bronze(df_races)
            .withColumn("data_source", lit(v_data_source))
            .withColumn("file_date", lit(v_file_date)))

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Write the output to processed container in parquet format
#df_races_selected.write.mode('overwrite').partitionBy('race_year').parquet(f"{bronze_folder_path}/races")

# COMMAND ----------

df_races.write.mode("overwrite").format("parquet").saveAsTable("f1_bronze.races")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
