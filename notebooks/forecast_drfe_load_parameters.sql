-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create Retailer_Client Widget

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Retailer_Client")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create ADLS_Account_Name Widget

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Days_To_Forecast Widget

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Days_To_Forecast", "", "Days_To_Forecast");

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Days_To_Forecast")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Read Retailer_Client and ADLS_Account_Name widget values in Scala local variables
-- MAGIC 
-- MAGIC These values are used by various Scala code

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val scala_Retailer_Client = dbutils.widgets.get("Retailer_Client")
-- MAGIC val scala_ADLS_Account_Name = dbutils.widgets.get("ADLS_Account_Name")
-- MAGIC val scala_Days_To_Forecast = dbutils.widgets.get("Days_To_Forecast")
-- MAGIC val scala_retailer = dbutils.widgets.get("Retailer_Client").split("_")(0);
-- MAGIC val scala_client = dbutils.widgets.get("Retailer_Client").split("_")(1);
-- MAGIC var scala_drfee_MAX_LAG = "14";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Refreshing the table so that files loaded from Data Vault processes are registered into Metastore

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_dv.AUDIT_DRIVER_SALES_DATES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit the notebook if there is no last processed timestamp present
-- MAGIC 
-- MAGIC This would potentially happen in the very first run where the process expects some value stored in table AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS to let the process know which dates to process.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${scala_Retailer_Client}_retail_alert_im.AUDIT_drfe_FORECAST_LAST_PROCESSED_TS")
-- MAGIC val count = df.count
-- MAGIC println(count)
-- MAGIC if (count == 0 ){
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create a temp table to hold LOAD_TS to process in current batch
-- MAGIC 
-- MAGIC We want to hold them at the beginning of the process so that when we update AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS at the end of the process, we do not have to refer back to AUDIT_DRIVER_SALES_DATES which may have new sales dates loaded.

-- COMMAND ----------

DROP TABLE IF EXISTS ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS;
CREATE TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS AS
SELECT
  A.SALES_DT,
  date_format(A.LOAD_TS, "yyyy-MM-dd hh:mm:ss") AS LOAD_TS
FROM
  ${Retailer_Client}_dv.AUDIT_DRIVER_SALES_DATES A,
  ${Retailer_Client}_retail_alert_im.AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS B
WHERE
A.LOAD_TS > B.LOAD_TS;
REFRESH TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Checking if there is no new dates to process, otherwise populate file with 0

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${scala_Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS")
-- MAGIC val count = df.count
-- MAGIC 
-- MAGIC val df_dv_max_sales_dt = spark.sql(f"SELECT max(sales_dt) FROM ${scala_Retailer_Client}_dv.vw_sat_link_epos_summary where sales_dt >= (SELECT max(sales_dt) FROM ${scala_Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS)").collect()(0).getDate(0)
-- MAGIC val df_batch_max_sales_dt = spark.sql(f"SELECT max(sales_dt) FROM ${scala_Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS").collect()(0).getDate(0)
-- MAGIC 
-- MAGIC println(count)
-- MAGIC if ((count == 0) || (df_batch_max_sales_dt.before(df_dv_max_sales_dt))){
-- MAGIC   // Prepare a line with 0 days to predict, this will be further used in ADF to not run the DRFE notebook at all
-- MAGIC   val df_params = spark.sql(f"SELECT '${scala_retailer}' AS RETAILER_PARAM, '${scala_client}' AS CLIENT_PARAM, '00010101' as LAST_DAY_TO_PREDICT_PARAM, '0' as NUM_DAYS_TO_PREDICT_PARAM");
-- MAGIC   df_params.show(false)
-- MAGIC   // Write the outputs of SQL query to a file
-- MAGIC   df_params.coalesce(1).write.mode("overwrite").json(f"/mnt/sadlsrccode_dataplatform-retail-alertgeneration/config/TEMP_${scala_Retailer_Client}_drfe")
-- MAGIC   // Drop the temp table TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS since this table is empty
-- MAGIC   spark.sql(f"DROP TABLE ${scala_Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS")
-- MAGIC } else {
-- MAGIC   // Prepare a line with parameters that will be further passed to the DRFE notebook
-- MAGIC   val df_params = spark.sql(f"SELECT '${scala_retailer}' AS RETAILER_PARAM, '${scala_client}' AS CLIENT_PARAM, date_format(DATE_ADD(max(sales_dt), ${scala_Days_To_Forecast}), 'yMMdd') as LAST_DAY_TO_PREDICT_PARAM, case when (datediff(DATE_ADD(max(sales_dt), ${scala_Days_To_Forecast}),max(sales_dt))) <= 14 then cast((datediff(DATE_ADD(max(sales_dt), ${scala_Days_To_Forecast}),max(sales_dt))) as string) else '${scala_drfee_MAX_LAG}' end as NUM_DAYS_TO_PREDICT_PARAM FROM ${scala_Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS");
-- MAGIC   df_params.show(false)
-- MAGIC   // Write the outputs of SQL query to a file
-- MAGIC   df_params.coalesce(1).write.mode("overwrite").json(f"/mnt/sadlsrccode_dataplatform-retail-alertgeneration/config/TEMP_${scala_Retailer_Client}_drfe")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC def writeDrfeJSONConfig(retailerClient: String, jsonConfigFilePath: String): Unit = {
-- MAGIC 
-- MAGIC   // This is the file name from your notebook while writing a DataFrame to BLOB
-- MAGIC   // I am using some temp name as I know we need to delete this directory anyways so its name does not matter
-- MAGIC   val jsonConfigFileDirName = f"TEMP_${retailerClient}_drfe/"
-- MAGIC 
-- MAGIC   // This is the actual JSON file name you want to store your DRFE notebook parameters
-- MAGIC   val finalJsonConfigFileName = f"dynamic_params_alert_generation_${retailerClient}_drfe.json"
-- MAGIC 
-- MAGIC   // Get the part* file name. Expecting 1 file always
-- MAGIC   val file = dbutils.fs.ls(jsonConfigFilePath + "/" + jsonConfigFileDirName).map(_.name).filter(_.startsWith("part"))(0)
-- MAGIC 
-- MAGIC   // Rename and part file to actual JSON file name we want and move it to /config (parent) directory
-- MAGIC   dbutils.fs.mv(jsonConfigFilePath  + "/" + jsonConfigFileDirName + file, jsonConfigFilePath + "/" + finalJsonConfigFileName)
-- MAGIC 
-- MAGIC 
-- MAGIC   // Remove the TEMP_${retailerClient}_drfe directory
-- MAGIC   dbutils.fs.rm(jsonConfigFilePath + jsonConfigFileDirName, true)
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Remove system generated files and rename the file 'TEMP_${scala_Retailer_Client}_drfe' to 'dynamic_params_alert_generation_[Retailer_Client]_drfe.json'
-- MAGIC writeDrfeJSONConfig(scala_Retailer_Client, "/mnt/sadlsrccode_dataplatform-retail-alertgeneration/config/")
