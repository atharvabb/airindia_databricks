// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

def generic_df_reader(spark_session: SparkSession, 
                      source_format: String = "parquet", 
                      path: String, 
                      schemaType:String,
                      structSchema: StructType=StructType(List()),
                      stringSchema:String="", 
                      extra_configs: Option[Map[String, Boolean]] = None): DataFrame = 
{
  if(schemaType.toLowerCase()!="ddlstring" && schemaType.toLowerCase()!="structtype"){
      throw new Exception(s"INVALID SCHEMA TYPE $schemaType ONLY DDLSTRING AND STRUCTTYPE SUPPORTED")
  }

  val supported_formats = List("text","csv","parquet","delta","orc","avro","json")
  if(!supported_formats.contains(source_format)){
      throw new Exception(s"INVALID File Format $source_format ONLY $supported_formats SUPPORTED")
  }

  var df:DataFrameReader=spark_session.read
  if(schemaType.toLowerCase()=="ddlstring"){
            df = df.format(source_format).schema(stringSchema).option("path", path)
        }
  else{
            df = df.format(source_format).schema(structSchema).option("path", path)
        }
  
    extra_configs.foreach(_.foreach { case (key, value) => df = df.option(key, value) })
    df.load()
}

