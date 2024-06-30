// Databricks notebook source
// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Custom_SparkSession_Create"

// COMMAND ----------

// MAGIC %run "/Workspace/Users/atharvaxa9@gmail.com/utilities/Generic_DF_Reader"

// COMMAND ----------

//Congifs for plane data cleaning
import org.apache.spark.sql.functions._
val citySchema = "id int,name string,countrycode string,district string,population long"

val cityDFOptions = Some(Map("header"->false,"mergeSchema"->true))

val curr_date = java.time.LocalDate.now
val sink_part_col = "dw_load_date"
val source_mnt_path = "/mnt/7x4/bronze/City"
val sink_mnt_path = "/mnt/7x4/silver/City"

//Spark Session Creation
val spark = Spark_Session_Create("local[*]","AirIndia_Cleaning",Some(Map("spark.sql.autoBroadcastJoinThreshold"->"100")))

// COMMAND ----------

val city_baseDF = generic_df_reader(spark,"parquet",source_mnt_path,"ddlstrinG",stringSchema=citySchema,extra_configs=cityDFOptions).filter(col("dw_load_date")===lit(curr_date))
city_baseDF.createOrReplaceTempView("cityDF_raw")


// COMMAND ----------

val target_df = spark.sql("select distinct * from bsl_airindia_silver.city")

val joined_city = city_baseDF.join(target_df,target_df.col("id")===city_baseDF.col("id"),"left")
val city_new_inserted = joined_city.filter(target_df.col("id").isNull)
                                  .select(city_baseDF.col("*"),lit(1).alias("is_active"))

val city_updated_all = joined_city.filter(target_df.col("id").isNotNull)
                              .filter(target_df.col("name")=!=city_baseDF.col("name") ||
                                     target_df.col("countrycode")=!=city_baseDF.col("countrycode") ||
                                     target_df.col("district")=!=city_baseDF.col("district")  ||
                                     target_df.col("population")=!=city_baseDF.col("population")
                                    )
val city_updated = city_updated_all.select(city_baseDF.col("*"),lit(1).alias("is_active"))
val city_updated_old = city_updated_all.select(target_df.col("*")).withColumn("is_active",lit(0))

val city_new_updated = city_new_inserted.unionByName(city_updated)
spark.sql(s"create table IF NOT EXISTS bsl_airindia_silver.city_update_insert")
city_new_updated.write.mode(SaveMode.Overwrite).option("mergeSchema",true)
                      .saveAsTable("bsl_airindia_silver.city_update_insert")

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO bsl_airindia_silver.city as city_target
// MAGIC USING bsl_airindia_silver.city_update_insert as cityDF_raw 
// MAGIC ON city_target.id=cityDF_raw.id 
// MAGIC WHEN MATCHED THEN
// MAGIC UPDATE SET name=cityDF_raw.name,
// MAGIC            countrycode=cityDF_raw.countrycode,
// MAGIC            district=cityDF_raw.district,
// MAGIC            population=cityDF_raw.population,
// MAGIC            is_active=cityDF_raw.is_active
// MAGIC WHEN NOT MATCHED THEN
// MAGIC INSERT (id,name,countrycode,district,population,is_active) VALUES 
// MAGIC        (cityDF_raw.id,cityDF_raw.name,cityDF_raw.countrycode,cityDF_raw.district,cityDF_raw.population,cityDF_raw.is_active)
// MAGIC
// MAGIC

// COMMAND ----------

city_updated_old.write.format("delta").mode(SaveMode.Append)
                               .partitionBy(sink_part_col)
                               .option("mergeSchema",true)
                               .save(sink_mnt_path)
