// Databricks notebook source
// MAGIC %sql
// MAGIC
// MAGIC select code,count(*) from bsl_airindia_silver.UNIQUE_CARRIERS group by code having count(*)>1
// MAGIC -- select * from bsl_airindia_silver.Flights;1
// MAGIC -- select * from bsl_airindia_silver.Airport;
// MAGIC -- select * from bsl_airindia_silver.Cancellation;1
// MAGIC -- select * from bsl_airindia_silver.Airplane_Api_Data;

// COMMAND ----------

import org.apache.spark.sql.functions._
val Flights_BaseDF = spark.read.format("delta").table("bsl_airindia_silver.Flights")
                     .drop("CRSElapsedTime","AirTime","TaxiIn","TaxiOut","ActualElapsedTime")
val uniq_carr_BaseDF = spark.read.format("delta").table("bsl_airindia_silver.UNIQUE_CARRIERS").drop("dw_load_date").distinct()
val cancel_code_BaseDF = spark.read.format("delta").table("bsl_airindia_silver.Cancellation").drop("dw_load_date").distinct()
val planesDF = spark.read.format("delta").table("bsl_airindia_silver.Planes").drop("dw_load_date").distinct()

// COMMAND ----------

val Flights_with_airline_name_DF = Flights_BaseDF
                                   .join(broadcast(uniq_carr_BaseDF),uniq_carr_BaseDF.col("Code")===Flights_BaseDF.col("UniqueCarrier"),"left")
                                   .drop("uniq_carr_BaseDF.Code","Flights_BaseDF.UniqueCarrier")
                                   .withColumnRenamed("Description","Airline_Name")
                                   .withColumnRenamed("TailNum","Tail_Num")
           

// COMMAND ----------

val Trans_Joined_Plane = Flights_with_airline_name_DF.join(broadcast(planesDF),planesDF.col("tailnum")===Flights_with_airline_name_DF.col("Tail_Num"),"left").drop("tailnum")


val Final_Trans_PreDF = Trans_Joined_Plane.join(broadcast(cancel_code_BaseDF),cancel_code_BaseDF.col("Code")===Trans_Joined_Plane.col("CancellationCode"),"left")
                         .drop("Code")
                         .withColumnRenamed("Description","Cancellation_Reason")

val final_trans_DF = Final_Trans_PreDF.select("FlightNum","Tail_Num","Origin","Dest","fly_date","DepTime","CRSDepTime","ArrTime","CRSArrTime","UniqueCarrier","Distance","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay","stg_id","Airline_Name","type","manufacturer","issue_date","model","status","aircraft_type","engine_type","dw_load_date")
 

// COMMAND ----------

final_trans_DF.write.format("delta")
                    .mode(SaveMode.Overwrite)
                    .partitionBy("dw_load_date","fly_date")
                    .option("mergeSchema",true)
                    .saveAsTable("bsl_airindia_gold.Reporting_7x4")
