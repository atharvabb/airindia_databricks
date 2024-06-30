// Databricks notebook source
dbutils.widgets.text("Layer_Name", "env_setup")
val process_req = dbutils.widgets.get("Layer_Name")

// COMMAND ----------

val WorkFlow:Map[String,Map[String,String]] = 
Map("env_setup"->
Map("mount_point_create"->"/Workspace/Users/atharvaxa9@gmail.com/utilities/Mount_Point_Creation",
    "db_table_creation"->"/Workspace/Users/atharvaxa9@gmail.com/utilities/Db_Tables_Creation",
   ),
   "cleaning"->
Map("api_data_cleaning"->"/Workspace/Users/atharvaxa9@gmail.com/Cleaning/Api_Data_Cleaning",
    "airport_w_can_code"->"/Workspace/Users/atharvaxa9@gmail.com/Cleaning/Airport_w_cancel_code_Cleaning",
    "city_cleaning"->"/Workspace/Users/atharvaxa9@gmail.com/Cleaning/City_Cleaning",
    "flights_cleaning"->"/Workspace/Users/atharvaxa9@gmail.com/Cleaning/Flights_Cleaning",
    "mysql_cleaning"->"/Workspace/Users/atharvaxa9@gmail.com/Cleaning/MySql_Tables_Cleaning",
    "planes_cleaning"->"/Workspace/Users/atharvaxa9@gmail.com/Cleaning/Planes_Cleaning"
   ),
   "dq_checks"->
Map("flights_dq_check"->"/Workspace/Users/atharvaxa9@gmail.com/Data_Quality_Checks/Data_Quality_Checks"),
   "transform"->
Map("reporting"->"/Workspace/Users/atharvaxa9@gmail.com/Transform/Reporting_7X4_Transformation")
   )
                  

// COMMAND ----------

//Run All Process
if(process_req.toUpperCase()=="ALL"){
    for((key,value)<-WorkFlow){
      for((job_name,notebook_path)<-value){
          dbutils.notebook.run(notebook_path,2000)
      }
    }
}
//Run Requested Process
else{
    for((key,value)<-WorkFlow){
      if(key==process_req.toLowerCase()){
      for((job_name,notebook_path)<-value){
          dbutils.notebook.run(notebook_path,2000)
      }
      }
    }
}
