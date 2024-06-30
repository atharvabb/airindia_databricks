// Databricks notebook source
//DATA QUALITY CHECKS TO DO IN THIS SHEET
//1)TO CHECK DATA VOLUME VARIATION FROM LAST DAY TO CURRENT DAY IF MORE THAN 10% RAISE ERROR AND STOP PROCESS
//CURRENLT DQ CHECK IS ONLY DONE FOR FLIGHTS FACT DATASET

// COMMAND ----------


val curr_date = java.time.LocalDate.now
//TS bsl_airindia_silver.Flights;


// COMMAND ----------

import org.apache.spark.sql.functions._ 
val df = spark.sql("describe history bsl_airindia_silver.Flights").filter(col("operation")===lit("WRITE"))
var sec_max_version:Int = 0
val max_version:Int = df.agg(max("version")).head()(0).toString.toInt
if(max_version>0){
   sec_max_version = max_version-1
}
else{
   sec_max_version = max_version
}

val max_count = df.filter(col("version")===lit(max_version))
                .select("operationMetrics.numOutputRows").head()(0).toString.toLong
val sec_max_count = df.filter(col("version")===lit(sec_max_version))
                .select("operationMetrics.numOutputRows").head()(0).toString.toLong

if(((max_count-sec_max_count)*100.0/sec_max_count)>10.00){
  throw new Exception(s"""Flights Count Exceeds the Data Quality Check limit of 10%\nActual count is ${(max_count-sec_max_count)*100.0/sec_max_count} where as expected count is less than 10.00%""")
}
else{
  print(s"FLIGHT COUNT DATA QUALITY CHECK PASSED")
}

