# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bsl_airindia_silver;
# MAGIC CREATE DATABASE IF NOT EXISTS bsl_airindia_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bsl_airindia_silver.city;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_silver.planes;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.planes;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_silver.UNIQUE_CARRIERS;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.UNIQUE_CARRIERS;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_silver.city;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.city;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_silver.Flights;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.flights;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_silver.Airport;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.Airport;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_silver.Cancellation;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.Cancellation;
# MAGIC DROP TABLE IF EXISTS  bsl_airindia_silver.Airplane_Api_Data;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.Airplane_Api_Data;
# MAGIC DROP TABLE IF EXISTS bsl_airindia_gold.Reporting_7x4;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_silver.planes USING DELTA LOCATION '/mnt/7x4/silver/Plane';
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_silver.UNIQUE_CARRIERS USING DELTA LOCATION '/mnt/7x4/silver/UNIQUE_CARRIERS';
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_silver.city USING DELTA LOCATION '/mnt/7x4/silver/City';
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_silver.Flights USING DELTA LOCATION '/mnt/7x4/silver/Flights';
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_silver.Airport USING DELTA LOCATION '/mnt/7x4/silver/Airport';
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_silver.Cancellation USING DELTA LOCATION '/mnt/7x4/silver/Cancellation';
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_silver.Airplane_Api_Data USING DELTA LOCATION '/mnt/7x4/silver/Airplane_Api_Data';
# MAGIC
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS bsl_airindia_gold.Reporting_7x4(
# MAGIC FlightNum integer,
# MAGIC Tail_Num string,
# MAGIC Origin string,
# MAGIC Dest string,
# MAGIC DepTime timestamp,
# MAGIC CRSDepTime integer,
# MAGIC ArrTime timestamp,
# MAGIC CRSArrTime integer,
# MAGIC UniqueCarrier string,
# MAGIC Distance integer,
# MAGIC Cancelled integer,
# MAGIC CancellationCode string,
# MAGIC Diverted integer,
# MAGIC CarrierDelay integer,
# MAGIC WeatherDelay integer,
# MAGIC NASDelay integer,
# MAGIC SecurityDelay integer,
# MAGIC LateAircraftDelay integer,
# MAGIC stg_id long,
# MAGIC Airline_Name string,
# MAGIC type string,
# MAGIC manufacturer string,
# MAGIC issue_date string,
# MAGIC model string,
# MAGIC status string,
# MAGIC aircraft_type string,
# MAGIC engine_type string
# MAGIC )PARTITIONED BY(dw_load_date date,fly_date date)
# MAGIC LOCATION '/mnt/7x4/gold/Reporting7x4';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bsl_airindia_silver.flights;
