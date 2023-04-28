# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, asc,desc, monotonically_increasing_id, concat, lit, max, min, row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

base_url = 'https://ergast.com/api/f1/'
end_year = 2023

# COMMAND ----------

# Define dataframe schenma
schema = StructType([
    StructField('code', StringType(), True),
    StructField('dateOfBirth', StringType(), True),
    StructField('driverId', StringType(), True),
    StructField('familyName', StringType(), True),
    StructField('givenName', StringType(), True),
    StructField('nationality', StringType(), True),
    StructField('permanentNumber', StringType(), True),
    StructField('url', StringType(), True),
    StructField('year', IntegerType(), True)
])

# Create dataframe that it'll receive the data
df_drivers = spark.createDataFrame([], schema)

# COMMAND ----------



# COMMAND ----------

# Creating a loop to do request data of each year since of the first ride
for year in range(1950, end_year+1):

    # Doing the consult to get all drivers
    response = requests.get(base_url + str(year) + '/drivers.json')
    json_data = response.json()

    # Getting the drivers list from JSON
    drivers_list = json_data['MRData']['DriverTable']['Drivers']

    # Convert the list to Dataframe Pyspark
    df_drivers_year = spark.createDataFrame(drivers_list)

    # Creating a new column to storage the year from request
    df_drivers_year = df_drivers_year.withColumn('year', F.lit(year))

    #In some years of the Formul1, we haven't the data so, to be able to do this project, I created a condition that check if the column exists in dataframe from that specific year.
    #if the column exist, I'll insert the values else I'll set this as null
    #I've understood the main columns that some year doesn't exists is code, givenName and permanentNumber
    if 'code' not in df_drivers_year.columns:
        df_drivers_year = df_drivers_year.withColumn('code', F.lit(None))
    if 'givenName' not in df_drivers_year.columns:
        df_drivers_year = df_drivers_year.withColumn('givenName', F.lit(None))
    if 'permanentNumber' not in df_drivers_year.columns:
        df_drivers_year = df_drivers_year.withColumn('permanentNumber', F.lit(None))


    df_drivers_year = df_drivers_year.select('code','dateOfBirth','driverId','familyName','givenName','nationality','permanentNumber','url','year')

    if df_drivers.isEmpty():
        df_drivers = df_drivers_year
    else:
        df_drivers = df_drivers.union(df_drivers_year)


# COMMAND ----------

df_drivers = df_drivers.orderBy(asc("driverId")).withColumn("SkDrivers",monotonically_increasing_id()+1)

# COMMAND ----------

df_drivers = add_date_load_landing(df_drivers)

# COMMAND ----------

df_drivers.write.mode("overwrite").parquet(f"{landing_folder_path}/drivers")
