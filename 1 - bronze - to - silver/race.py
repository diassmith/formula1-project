# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with races.csv file

# COMMAND ----------

# DBTITLE 1,Creating parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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
df_races = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{bronze_folder_path}/races.csv")

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Creating new column
df_races = add_date_load(df_races)

# COMMAND ----------

# DBTITLE 1,Creating new column
#creating new column raca_timestamp, it's a column that contains the combined value between data and time column
df_races = df_races.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Selecting the all columns that I'll use.
df_races_selected = df_races.select(col('raceId').alias('race_id'), col('year').alias('race_year'),col('round'), col('circuitId').alias('circuit_id'),col('name'), col('date_load'), col('race_timestamp'))

# COMMAND ----------

display(df_races_selected)

# COMMAND ----------

# DBTITLE 1,Write the output to processed container in parquet format
#df_races_selected.write.mode('overwrite').partitionBy('race_year').parquet(f"{silver_folder_path}/races")

# COMMAND ----------

df_races_selected.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.races")

# COMMAND ----------

dbutils.notebook.exit("Sucess")
