# Databricks notebook source
# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# base_url = 'https://ergast.com/api/f1/'
# end_year = 2023

# COMMAND ----------


# from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
# schema = StructType([
#     StructField("Circuit", MapType(StringType(), StringType()), True),
#     StructField("Results", ArrayType(
#         StructType([
#             StructField("element", MapType(StringType(), StringType()), True),
#             StructField("date", StringType(), True),
#             StructField("raceName", StringType(), True),
#             StructField("round", StringType(), True),
#             StructField("season", StringType(), True),
#             StructField("time", StringType(), True),
#             StructField("url", StringType(), True)
#         ])
#     ), True)
# ])

# # Create dataframe that it'll receive the data
# df_results = spark.createDataFrame([], schema)

# COMMAND ----------

# # Creating a loop to do request data of each year since of the first ride
# for year in range(1950, end_year+1):

#     # Doing the consult to get all drivers
#     response = requests.get(base_url + str(year) + '/results.json?limit=99999')
#     json_data = response.json()

#     # Getting the drivers list from JSON
#     results_list = json_data['MRData']['RaceTable']['Races']

#     # Convert the list to Dataframe Pyspark
#     df_results_year = spark.createDataFrame(results_list)

#     # Creating a new column to storage the year from request
#     # #Creating a new column id using hash
#     # df_results_year = (df_drivers_year.withColumn('year', F.lit(year))
#     #                                   .withColumn('id', abs(hash(concat("driverId", df_drivers_year["driverId"])))))

#     #In some years of the Formul1, we haven't the data so, to be able to do this project, I created a condition that check if the column exists in dataframe from that specific year.
#     #if the column exist, I'll insert the values else I'll set this as null
#     #I've understood the main columns that some year doesn't exists is code, givenName and permanentNumber
#     if 'time' not in df_results_year.columns:
#         df_results_year = df_results_year.withColumn('time', F.lit(None))

#     if df_results.isEmpty():
#         df_results = df_results_year
#     else:
#         df_results = df_results.union(df_results_year)


# COMMAND ----------



# Define os anos de início e fim
start_year = 1950
end_year = 2024

# Consulta a API e obtém os dados em formato JSON para cada ano
json_data = []
for year in range(start_year, end_year):
    url = f'https://ergast.com/api/f1/{year}/results.json?limit=99999'
    response = requests.get(url)
    json_data.append(response.json())

# Extrai os resultados da resposta e cria um DataFrame
rows = []
for data in json_data:
    results = data['MRData']['RaceTable']['Races']
    for race in results:
        row = {}
        row['date'] = race['date']
        row['raceName'] = race['raceName']
        row['season'] = race['season']
        for result in race['Results']:
            row['position'] = result['position']
            row['points'] = result['points']
            row['positionText'] = result['positionText']
            row['grid'] = result['grid']
            row['laps'] = result['laps']
            row['status'] = result['status']
            row['driver_driverId'] = result['Driver']['driverId']
            row['driver_familyName'] = result['Driver']['familyName']
            row['driver_givenName'] = result['Driver']['givenName']
            row['driver_nationality'] = result['Driver']['nationality']
            row['constructor_constructorId'] = result['Constructor']['constructorId']
            row['constructor_name'] = result['Constructor']['name']
            row['constructor_nationality'] = result['Constructor']['nationality']
            rows.append(row.copy())

# Cria o DataFrame a partir da lista de linhas
df_results = spark.createDataFrame(rows)

df_results = (df_results.withColumn('raceId', abs(hash( df_results["raceName"]))))

df_results = (df_results.withColumn('driverId', abs(hash( df_results["driver_driverId"]))))

df_results = (df_results.withColumn('constructorId', abs(hash( df_results["constructor_constructorId"]))))

df_results = (df_results.withColumn('resultId', concat(concat(concat('raceId','driverId'),'constructorId'),abs(hash( df_results["status"])))))
                        # .withColumn('driverId2',abs(hash(concat("driverId", df_results["driver_driverId"]))))
                        # .withColumn('constructorId2', abs(hash(concat("constructorId", df_results["constructor_constructorId"])))))
# Transforma a coluna 'date' em tipo 'date'
# df_results = df_results.withColumn('date', F.to_date('date'))

# # Exibe o DataFrame resultante
# df_results.show()


# COMMAND ----------

display(df_results)

# COMMAND ----------

# display(df_results.filter('date = "2023-05-07"'))

# COMMAND ----------

df_results.write.mode("overwrite").parquet(f"{landing_folder_path}/results")
