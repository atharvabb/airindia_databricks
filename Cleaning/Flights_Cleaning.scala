// Databricks notebook source
// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Custom_SparkSession_Create"

// COMMAND ----------

// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Generic_DF_Reader"

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val flights_schema= StructType(Array(
  StructField("Year", IntegerType, nullable = true),
  StructField("Month", IntegerType, nullable = true),
  StructField("DayofMonth", IntegerType, nullable = true),
  StructField("DayOfWeek", IntegerType, nullable = true),
  StructField("DepTime", IntegerType, nullable = true),
  StructField("CRSDepTime", IntegerType, nullable = true),
  StructField("ArrTime", IntegerType, nullable = true),
  StructField("CRSArrTime", IntegerType, nullable = true),
  StructField("UniqueCarrier", StringType, nullable = true),
  StructField("FlightNum", IntegerType, nullable = true),
  StructField("TailNum", StringType, nullable = true),
  StructField("ActualElapsedTime", IntegerType, nullable = true),
  StructField("CRSElapsedTime", IntegerType, nullable = true),
  StructField("AirTime", IntegerType, nullable = true),
  StructField("ArrDelay", IntegerType, nullable = true),
  StructField("DepDelay", IntegerType, nullable = true),
  StructField("Origin", StringType, nullable = true),
  StructField("Dest", StringType, nullable = true),
  StructField("Distance", IntegerType, nullable = true),
  StructField("TaxiIn", IntegerType, nullable = true),
  StructField("TaxiOut", IntegerType, nullable = true),
  StructField("Cancelled", IntegerType, nullable = true),
  StructField("CancellationCode", StringType, nullable = true),
  StructField("Diverted", IntegerType, nullable = true),
  StructField("CarrierDelay", IntegerType, nullable = true),
  StructField("WeatherDelay", IntegerType, nullable = true),
  StructField("NASDelay", IntegerType, nullable = true),
  StructField("SecurityDelay", IntegerType, nullable = true),
  StructField("LateAircraftDelay", IntegerType, nullable = true)
))

val cityDFOptions = Some(Map("header"->true,"mergeSchema"->true))

val curr_date = java.time.LocalDate.now
val sink_part_col = "dw_load_date"
val source_mnt_path = "/mnt/7x4/bronze/Flights"
val sink_mnt_path = "/mnt/7x4/silver/Flights"

//Spark Session Creation
val spark = Spark_Session_Create("local[*]","AirIndia_Cleaning",Some(Map("spark.sql.autoBroadcastJoinThreshold"->"100")))


// COMMAND ----------

val flights_baseDF = generic_df_reader(spark,"csv",source_mnt_path,"structType",structSchema=flights_schema,extra_configs=cityDFOptions).filter(col("dw_load_date")===lit(curr_date))


// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
val trans_flights = flights_baseDF
                    .withColumn("fly_date",to_date(concat(col("Year"),lit("-"),col("Month"),lit("-"),col("DayofMonth")),"yyyy-MM-dd"))
                    .withColumn("ArrTime",col("ArrTime").cast("string"))
                    .withColumn("ArrTime",concat(substring(col("ArrTime"),1,2),lit(":"),substring(col("ArrTime"),3,4)))
                    .withColumn("ArrTime",to_timestamp(concat(col("fly_date"),lit(" "),col("ArrTime")),"yyyy-MM-dd HH:mm"))
                    .withColumn("DepTime",col("DepTime").cast("string"))
                    .withColumn("DepTime",concat(substring(col("DepTime"),1,2),lit(":"),substring(col("DepTime"),3,4)))
                    .withColumn("DepTime",to_timestamp(concat(col("fly_date"),lit(" "),col("DepTime")),"yyyy-MM-dd HH:mm"))
                    .withColumn("stg_id",monotonically_increasing_id())
                    .drop("Year","Month","DayofMonth","DayofWeek")



// COMMAND ----------

trans_flights.write.format("delta")
                   .partitionBy("dw_load_date")
                   .option("mergeSchema",true)
                   .option("overwriteSchema", true)
                   .mode(SaveMode.Overwrite)
                   .save(sink_mnt_path)
