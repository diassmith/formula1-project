# Databricks notebook source
# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw

# COMMAND ----------

# DBTITLE 1,Show databases
# MAGIC %sql
# MAGIC SHOW databases

# COMMAND ----------

# MAGIC %sql
# MAGIC Use f1_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE f1_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select CURRENT_DATABASE()

# COMMAND ----------

# DBTITLE 1,Create Circuits Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.circuits(circuitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country STRING,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/adlsformula1/bronze/circuits.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_bronze.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.circuits

# COMMAND ----------

# DBTITLE 1,Create Races Table 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.races(raceId INT,
# MAGIC year INT,
# MAGIC round INT,
# MAGIC circuitId INT,
# MAGIC name STRING,
# MAGIC date DATE,
# MAGIC time STRING,
# MAGIC url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/adlsformula1/bronze/races.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.races

# COMMAND ----------

# DBTITLE 1,Create contructors table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.constructors(
# MAGIC constructorId INT,
# MAGIC constructorRef STRING,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "/mnt/adlsformula1/bronze/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.constructors

# COMMAND ----------

# DBTITLE 1,Create Drivers Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.drivers(
# MAGIC driverId INT,
# MAGIC driverRef STRING,
# MAGIC number INT,
# MAGIC code STRING,
# MAGIC name STRUCT<forename: STRING, surname: STRING>,
# MAGIC dob DATE,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS (path "/mnt/adlsformula1/bronze/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.drivers

# COMMAND ----------

# DBTITLE 1,Create Results Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.results(
# MAGIC resultId INT,
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC constructorId INT,
# MAGIC number INT,grid INT,
# MAGIC position INT,
# MAGIC positionText STRING,
# MAGIC positionOrder INT,
# MAGIC points INT,
# MAGIC laps INT,
# MAGIC time STRING,
# MAGIC milliseconds INT,
# MAGIC fastestLap INT,
# MAGIC rank INT,
# MAGIC fastestLapTime STRING,
# MAGIC fastestLapSpeed FLOAT,
# MAGIC statusId STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "/mnt/adlsformula1/bronze/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.results

# COMMAND ----------

# DBTITLE 1,Create Piststops Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.pit_stops(
# MAGIC driverId INT,
# MAGIC duration STRING,
# MAGIC lap INT,
# MAGIC milliseconds INT,
# MAGIC raceId INT,
# MAGIC stop INT,
# MAGIC time STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "/mnt/adlsformula1/bronze/pit_stops.json", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.pit_stops

# COMMAND ----------

# DBTITLE 1,Create lap_times table 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.lap_times(
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC lap INT,
# MAGIC position INT,
# MAGIC time STRING,
# MAGIC milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "/mnt/adlsformula1/bronze/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.lap_times

# COMMAND ----------

# DBTITLE 1,Create qualifying table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_bronze.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.qualifying(
# MAGIC constructorId INT,
# MAGIC driverId INT,
# MAGIC number INT,
# MAGIC position INT,
# MAGIC q1 STRING,
# MAGIC q2 STRING,
# MAGIC q3 STRING,
# MAGIC qualifyId INT,
# MAGIC raceId INT)
# MAGIC USING json
# MAGIC OPTIONS (path "/mnt/adlsformula1/bronze//qualifying", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_bronze.qualifying
