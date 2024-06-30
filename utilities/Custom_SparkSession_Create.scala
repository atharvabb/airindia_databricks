// Databricks notebook source
import org.apache.spark.sql.SparkSession
def Spark_Session_Create(Master_Name:String,App_Name:String,configs:Option[Map[String,String]]):SparkSession={
  
    if(Master_Name=="yarn" || Master_Name=="local[*]"||Master_Name=="kubernetes"){
  val spark = SparkSession.builder()
                     .master(Master_Name)
                     .appName(App_Name)
                     .getOrCreate()
  if(configs.getOrElse("")!=""){
    for((k,v)<-configs.get){
        
        spark.conf.set(k,v)
    }
  }
  spark
  }
  else{
    throw new Exception(s"Master $Master_Name Not Supported")
  }

}

