-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Create widgets to read required Input

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Read Retailer Client list text file on ADLS
-- MAGIC This file contains a list of all retailer-client combinations for which DB objects need to be created

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("List_File_Name", "", "List_File_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("List_File_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Drop_Create_All_Y_N_Flag", "N", "Drop_Create_All_Y_N_Flag")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Drop_Create_All_Y_N_Flag")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit if Drop_Create_All_Y_N_Flag is not Y or N

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (! List("y", "n").contains(dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase())){
-- MAGIC   dbutils.notebook.exit("Error: Invalid value found for Drop_Create_All_Y_N_Flag. Valid values are 'Y' and 'N'")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Read DDL Template Notebook
-- MAGIC This notebook contains parameterized DDLs for all the objects. The parameter is the name of the databases.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("DDL_Template_Notebook", "", "DDL_Template_Notebook")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("DDL_Template_Notebook")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Define a method to create DB objects for given list of Retailer-Clients

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC /**
-- MAGIC This method reads a List of strings as retailer-client combinations and
-- MAGIC calls a given notebook in loop to create respective DB objects
-- MAGIC */
-- MAGIC def createRetailerClientDBObjects(retailerClient: List[String]): Unit = {
-- MAGIC   
-- MAGIC   if (retailerClient.length == 0)
-- MAGIC     dbutils.notebook.exit("Error: Given list of Retailer-Client is empty")
-- MAGIC   
-- MAGIC   retailerClient.foreach(retailer_client => {
-- MAGIC 
-- MAGIC   // Create a Map to pass to the notebook as argument / widget parameter value
-- MAGIC   var args = Map("Retailer_Client" -> retailer_client)
-- MAGIC   args += ("ADLS_Account_Name" -> dbutils.widgets.get("ADLS_Account_Name"))
-- MAGIC   args += ("Drop_Create_All_Y_N_Flag" -> dbutils.widgets.get("Drop_Create_All_Y_N_Flag"))
-- MAGIC   println(s"Creating DB objects for '$retailer_client'")
-- MAGIC 
-- MAGIC   var ret = dbutils.notebook.run(dbutils.widgets.get("DDL_Template_Notebook"), 2000, args)
-- MAGIC 
-- MAGIC   println(ret)
-- MAGIC   println(s"'$retailer_client' DONE....")
-- MAGIC   })
-- MAGIC   dbutils.notebook.exit("")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Method getRetailerClientList to get List of Retailer-Client from given ADLS file

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC /**
-- MAGIC This method reads an absolute ADLS path and returns its content as a List of strings.
-- MAGIC It assumea there is only one column in the file and multiple rows representing
-- MAGIC the names of retailer-client combinations
-- MAGIC */
-- MAGIC def getRetailerClientList(adlsFile: String): List[String] = {
-- MAGIC   
-- MAGIC   // Read the given retailer client list file from ADLS
-- MAGIC   val df_retClientListFile = spark.read.format("csv")
-- MAGIC       .option("header", "false")
-- MAGIC       .load(adlsFile)
-- MAGIC   
-- MAGIC   // Return the content as a list of strings
-- MAGIC   df_retClientListFile.select("_c0").map(r => r.getString(0)).collect.toList  
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC createRetailerClientDBObjects(getRetailerClientList(dbutils.widgets.get("List_File_Name")))

-- COMMAND ----------

