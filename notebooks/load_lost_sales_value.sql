-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")
-- MAGIC dbutils.widgets.text("recordSourceCode", "", "recordSourceCode")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Retailer_Client")
-- MAGIC dbutils.widgets.get("recordSourceCode")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Read Retailer_Client widget values in Scala local variables
-- MAGIC 
-- MAGIC These values are used by various Scala code

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val retailerClient = dbutils.widgets.get("Retailer_Client")
-- MAGIC val recordSourceCode = dbutils.widgets.get("recordSourceCode")

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
-- MAGIC This would potentially happen in the very first run where the process expects some value stored in table AUDIT_LOESS/DRFE_LOST_SALES_LAST_PROCESSED_TS to let the process know which dates to process.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${retailerClient}_retail_alert_im.AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS")
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
-- MAGIC We want to hold them at the beginning of the process so that when we update AUDIT_LOESS/DRFE_LOST_SALES_LAST_PROCESSED_TS at the end of the process, we do not have to refer back to AUDIT_DRIVER_SALES_DATES which may have new sales dates loaded.

-- COMMAND ----------

DROP TABLE IF EXISTS ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS;
CREATE TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS AS
SELECT
  A.SALES_DT,
  date_format(A.LOAD_TS, "yyyy-MM-dd hh:mm:ss") AS LOAD_TS
FROM
  ${Retailer_Client}_dv.AUDIT_DRIVER_SALES_DATES A,
  ${Retailer_Client}_retail_alert_im.AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS B
WHERE
A.LOAD_TS > B.LOAD_TS;
REFRESH TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit the notebook if there is no new dates to process

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${retailerClient}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS")
-- MAGIC val count = df.count
-- MAGIC println(count)
-- MAGIC if (count == 0 ){
-- MAGIC   // Drop the temp table TMP_HOLD_AUDIT_LOESS/DRFE_FORECAST_LAST_PROCESSED_TS before exiting the notebook
-- MAGIC   spark.sql(f"DROP TABLE ${retailerClient}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS")
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Method: GetDates
-- MAGIC 
-- MAGIC This method reads unique SALES_DT from temp table TMP_HOLD_AUDIT_LOESS/DRFE_LOST_SALES_LAST_PROCESSED_TS and returns these dates as a list of string.
-- MAGIC These are the dates for which LSV needs to be calculated.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC def GetDates(): List[String] = {
-- MAGIC 
-- MAGIC   // Define SQL statement to get future dates
-- MAGIC   val sqlGetDate = f"""SELECT DISTINCT CAST(SALES_DT AS STRING) AS SALES_DT FROM ${retailerClient}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS """
-- MAGIC   
-- MAGIC   // Run the SQL statement
-- MAGIC   val result = spark.sql(sqlGetDate)
-- MAGIC   
-- MAGIC   // Return the dates from query result as a List of Strings
-- MAGIC   result.select("SALES_DT").map(r => r.getString(0)).collect.toList.sorted
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Method: Load_LOST_SALES_VALUE
-- MAGIC This method reads a date as an argument and runs a SQL query to load table LOST_SALES_VALUE

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC def Load_$recordSourceCode_LOST_SALES_VALUE(SalesDate: String): Unit = {
-- MAGIC // Define query to load table TMP_LOESS/DRFE_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA
-- MAGIC //SET spark.sql.shuffle.partitions=10;  
-- MAGIC    
-- MAGIC   spark.conf.set("spark.sql.shuffle.partitions","10")
-- MAGIC    val sql = f""" WITH CALC_LSV AS 
-- MAGIC    (
-- MAGIC 	
-- MAGIC     SELECT
-- MAGIC 	  vw_sat_lnk.HUB_Organization_Unit_HK,
-- MAGIC 	  vw_sat_lnk.HUB_Retailer_Item_HK,
-- MAGIC 	  CURRENT_TIMESTAMP AS LOAD_TS,
-- MAGIC 	  CASE 
-- MAGIC         WHEN vw_sat_unt_prc.UNIT_PRICE_AMT IS NULL THEN 0
-- MAGIC         WHEN fcst_unt.BASELINE_POS_ITEM_QTY IS NULL THEN 0
-- MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY IS NULL THEN 0
-- MAGIC 	    WHEN vw_sat_lnk.POS_ITEM_QTY >= fcst_unt.BASELINE_POS_ITEM_QTY THEN 0
-- MAGIC 	    WHEN vw_sat_lnk.POS_ITEM_QTY < fcst_unt.BASELINE_POS_ITEM_QTY THEN (fcst_unt.BASELINE_POS_ITEM_QTY - vw_sat_lnk.POS_ITEM_QTY) * vw_sat_unt_prc.UNIT_PRICE_AMT
-- MAGIC 	  ELSE 0 END AS LOST_SALES_AMT,
-- MAGIC 	  vw_sat_lnk.sales_dt AS SALES_DT,
-- MAGIC       "${recordSourceCode}" AS RECORD_SOURCE_CD
-- MAGIC 	
-- MAGIC     FROM ${retailerClient}_dv.vw_sat_link_epos_summary vw_sat_lnk 
-- MAGIC 	INNER JOIN ${retailerClient}_dv.VW_SAT_RETAILER_ITEM_UNIT_PRICE vw_sat_unt_prc
-- MAGIC 	  ON vw_sat_lnk.HUB_ORGANIZATION_UNIT_HK = vw_sat_unt_prc.HUB_ORGANIZATION_UNIT_HK and 
-- MAGIC 	  vw_sat_lnk.HUB_RETAILER_ITEM_HK = vw_sat_unt_prc.HUB_RETAILER_ITEM_HK and 
-- MAGIC 	  vw_sat_lnk.sales_dt = vw_sat_unt_prc.sales_dt 
-- MAGIC 	
-- MAGIC     INNER JOIN ${retailerClient}_retail_alert_im.${recordSourceCode}_FORECAST_BASELINE_UNIT fcst_unt 
-- MAGIC 	  ON fcst_unt.sales_dt = vw_sat_lnk.sales_dt and 
-- MAGIC 	  fcst_unt.HUB_Organization_Unit_HK = vw_sat_lnk.HUB_Organization_Unit_HK and 
-- MAGIC 	  fcst_unt.HUB_Retailer_Item_HK = vw_sat_lnk.HUB_Retailer_Item_HK
-- MAGIC     
-- MAGIC     WHERE vw_sat_lnk.sales_dt = '${SalesDate}'
-- MAGIC     )
-- MAGIC     INSERT INTO ${retailerClient}_retail_alert_im.LOST_SALES_VALUE PARTITION(SALES_DT, RECORD_SOURCE_CD)
-- MAGIC     SELECT * FROM CALC_LSV calc_lsv
-- MAGIC     WHERE 
-- MAGIC       NOT EXISTS
-- MAGIC       (
-- MAGIC         SELECT 1 FROM ${retailerClient}_retail_alert_im.LOST_SALES_VALUE LSV
-- MAGIC         WHERE 
-- MAGIC           LSV.SALES_DT = calc_lsv.SALES_DT AND
-- MAGIC           LSV.HUB_Organization_Unit_HK = calc_lsv.HUB_Organization_Unit_HK AND
-- MAGIC           LSV.HUB_Retailer_Item_HK = calc_lsv.HUB_Retailer_Item_HK AND
-- MAGIC           LSV.RECORD_SOURCE_CD = calc_lsv.RECORD_SOURCE_CD
-- MAGIC       )"""
-- MAGIC   
-- MAGIC   // Run the SQL statement
-- MAGIC   spark.sql(sql)
-- MAGIC }  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Load LSV
-- MAGIC Loads the target table LOST_SALES_VALUE and collect statistics for each of the dates

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC for (date <- GetDates()){
-- MAGIC     println(f"Running for Date - $date")
-- MAGIC     Load_$recordSourceCode_LOST_SALES_VALUE(date)
-- MAGIC     spark.sql(f"ANALYZE TABLE ${retailerClient}_retail_alert_im.LOST_SALES_VALUE PARTITION(SALES_DT = '${date}', RECORD_SOURCE_CD = '${recordSourceCode}') COMPUTE STATISTICS FOR COLUMNS HUB_Organization_Unit_HK, HUB_Retailer_Item_HK")
-- MAGIC     println(f"DONE for Date - $date")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Update the latest LOAD_TS consumed in this batch back to AUDIT_LOESS_LOST_SALES_LAST_PROCESSED_TS
-- MAGIC 
-- MAGIC This updated LOAD_TS will be used by the next batch to identify dates to forecast

-- COMMAND ----------

INSERT OVERWRITE TABLE ${Retailer_Client}_retail_alert_im.AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS
SELECT
  MAX(LOAD_TS) AS LOAD_TS,
  date_format(current_timestamp, "yyyy-MM-dd hh:mm:ss") AS CREATE_TS
FROM
  ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Drop Temp HOLD table

-- COMMAND ----------

DROP TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_LOST_SALES_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Refresh table LOST_SALES_VALUE

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.LOST_SALES_VALUE;
