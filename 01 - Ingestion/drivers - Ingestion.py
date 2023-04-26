# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

# COMMAND ----------

response = requests.get('http://ergast.com/api/f1/1964/drivers.json')
json_data = response.json()

df_drivers = spark.createDataFrame(json_data['MRData']['DriverTable']['Drivers'])


# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# Define o esquema com as colunas desejadas
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

# Cria um DataFrame vazio com o esquema definido
df_drivers = spark.createDataFrame([], schema)

# COMMAND ----------

if df_drivers.isEmpty():
    print("O dataframe está vazio")
else:
    print("O dataframe contém registros")

# COMMAND ----------

import requests
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

base_url = 'https://ergast.com/api/f1/'
end_year = 1964

# Define o esquema com as colunas desejadas
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

# Cria um DataFrame vazio com o esquema definido
df_drivers = spark.createDataFrame([], schema)

# Itera sobre um range de anos desde o início da F1 até o ano de interesse
for year in range(1960, end_year+1):

    # Faz a consulta para obter todos os drivers para o ano especificado
    response = requests.get(base_url + str(year) + '/drivers.json')
    json_data = response.json()

    # Obtém a lista de drivers a partir do JSON
    drivers_list = json_data['MRData']['DriverTable']['Drivers']

    # Converte a lista de drivers em um DataFrame do PySpark
    df_drivers_year = spark.createDataFrame(drivers_list)

    # Adiciona uma coluna com o ano correspondente
    df_drivers_year = df_drivers_year.withColumn('year', F.lit(year))

    if df_drivers.isEmpty():
        df_drivers = df_drivers_year
    else:
        df_drivers = df_drivers.union(df_drivers_year)

    # Adiciona o DataFrame do ano à lista de DataFrames
    #df_drivers = df_drivers.union(df_drivers_year)

# Exibe o DataFrame resultante
display(df_drivers)
#df_drivers_year.show()


# COMMAND ----------

display(df_drivers)
