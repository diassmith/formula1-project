# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with races.csv file

# COMMAND ----------

# DBTITLE 1,Importing Library and Functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

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
.csv("/mnt/adlsformula1/raw/races.csv")

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Creating new columns
#creating new column data_load
#creating new column raca_timestamp, it's a column that contains the combined value between data and time column

df_races = df_races.withColumn("date_load", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(df_races)

# COMMAND ----------

# DBTITLE 1,Selecting the all columns that I'll use.
df_races_selected = df_races.select(col('raceId').alias('race_id'), col('year').alias('race_year'),col('round'), col('circuitId').alias('circuit_id'),col('name'), col('date_load'), col('race_timestamp'))

# COMMAND ----------

display(df_races_selected)

# COMMAND ----------

# DBTITLE 1,Write the output to processed container in parquet format
df_races_selected.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/adlsformula1/processed/races')
