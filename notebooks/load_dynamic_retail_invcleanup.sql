-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")
-- MAGIC dbutils.widgets.text("recordSourceCode", "", "recordSourceCode")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Read Retailer_Client and ADLS_Account_Name widget values in Scala local variables
-- MAGIC 
-- MAGIC These values are used by various Scala code

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val Retailer_Client = dbutils.widgets.get("Retailer_Client")
-- MAGIC val ADLS_Account_Name = dbutils.widgets.get("ADLS_Account_Name")
-- MAGIC val recordSourceCode = dbutils.widgets.get("recordSourceCode")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit the notebook if there is no last processed timestamp present
-- MAGIC 
-- MAGIC This would potentially happen in the very first run where the process expects some value stored in table AUDIT_ALERTS_INVCLEANUP_LAST_PROCESSED_TS to let the process know which dates to process.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${Retailer_Client}_retail_alert_im.AUDIT_${recordSourceCode}_ALERTS_INVCLEANUP_LAST_PROCESSED_TS")
-- MAGIC val count = df.count
-- MAGIC println(count)
-- MAGIC if (count == 0 ){
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Temp hold of SALES_DT and LOAD_TS to process

-- COMMAND ----------

DROP TABLE IF EXISTS ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_invcleanup_sales_dt;
CREATE TABLE ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_invcleanup_sales_dt AS
SELECT
  SALES_DT
  , LOAD_TS
FROM (SELECT
        a.SALES_DT
        , a.LOAD_TS
        , ROW_NUMBER() OVER(PARTITION BY a.SALES_DT ORDER BY a.LOAD_TS DESC) rnum
      FROM ${Retailer_Client}_dv.audit_driver_sales_dates a
      INNER JOIN ${Retailer_Client}_retail_alert_im.audit_${recordSourceCode}_alerts_invcleanup_last_processed_ts b
        ON a.LOAD_TS > b.LOAD_TS) a
WHERE
  rnum = 1;
  

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_invcleanup_sales_dt;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit the notebook if there is no new dates to process

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_INVCLEANUP_SALES_DT")
-- MAGIC val count = df.count
-- MAGIC println(count)
-- MAGIC if (count == 0 ){
-- MAGIC   println("No dates to process, Exiting the notebook...")
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Get list of dates from TMP Hold table a string to pass to dynamic SQLs

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val dateList: String = spark.sql(f"SELECT * FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_INVCLEANUP_SALES_DT").collect.map("'" + _(0) + "'").mkString(",")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Get list of past 21 days for all dates in TMP Hold

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val last21DateLiilst = spark.sql(f"""SELECT DISTINCT
-- MAGIC     DATE_ADD(TMP.START_SALES_DATE,PE.i) AS FUTURE_SALES_DT
-- MAGIC FROM
-- MAGIC       (
-- MAGIC         SELECT DATE_ADD(SALES_DT, -21) AS START_SALES_DATE,
-- MAGIC                DATE_ADD(SALES_DT, 0) AS END_SALES_DATE
-- MAGIC         FROM
-- MAGIC              ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_INVCLEANUP_SALES_DT B
-- MAGIC       ) TMP 
-- MAGIC   LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.END_SALES_DATE,TMP.START_SALES_DATE)),' ')) PE AS i, X ORDER BY FUTURE_SALES_DT""").collect.map("'" + _(0) + "'").mkString(",")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Derive ICSP

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val sqlICSP = f"""
-- MAGIC SELECT DISTINCT
-- MAGIC   sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , sles.HUB_RETAILER_ITEM_HK
-- MAGIC   , sles.SALES_DT
-- MAGIC   , sles.ON_HAND_INVENTORY_QTY
-- MAGIC   , slesup.UNIT_PRICE_AMT
-- MAGIC FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
-- MAGIC INNER JOIN ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_INVCLEANUP_SALES_DT adsd
-- MAGIC   ON sles.SALES_DT = adsd.SALES_DT
-- MAGIC   AND sles.SALES_DT in (${dateList})
-- MAGIC INNER JOIN ${Retailer_Client}_dv.vw_sat_retailer_item_unit_price slesup
-- MAGIC   ON sles.HUB_ORGANIZATION_UNIT_HK = slesup.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   AND sles.HUB_RETAILER_ITEM_HK = slesup.HUB_RETAILER_ITEM_HK
-- MAGIC   AND sles.SALES_DT = slesup.SALES_DT
-- MAGIC   AND slesup.SALES_DT in (${dateList})  
-- MAGIC   
-- MAGIC WHERE
-- MAGIC   sles.ON_HAND_INVENTORY_QTY > 0
-- MAGIC """
-- MAGIC val icspDF = spark.sql(sqlICSP)
-- MAGIC 
-- MAGIC // Caching just to be onsafer side. If more than one action called on this df then and only then it will be cashed and not re-derived on 2nd action
-- MAGIC icspDF.cache
-- MAGIC icspDF.createOrReplaceTempView("icsp")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Derive ICSPWAU

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val sqlICSPWAU = f"""
-- MAGIC SELECT
-- MAGIC   sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , sles.HUB_RETAILER_ITEM_HK
-- MAGIC   , icsp.SALES_DT
-- MAGIC   , AVG(CASE WHEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0) > nvl(sles.POS_ITEM_QTY, 0)
-- MAGIC           THEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0)
-- MAGIC           ELSE nvl(sles.POS_ITEM_QTY, 0)
-- MAGIC         END) * 7 AS WEEKLY_AVG_POS_ITEM_QTY
-- MAGIC FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
-- MAGIC INNER JOIN icsp
-- MAGIC     ON sles.HUB_ORGANIZATION_UNIT_HK = icsp.HUB_ORGANIZATION_UNIT_HK
-- MAGIC     AND sles.HUB_RETAILER_ITEM_HK = icsp.HUB_RETAILER_ITEM_HK
-- MAGIC     AND sles.SALES_DT <= icsp.SALES_DT
-- MAGIC     AND sles.SALES_DT >= DATE_ADD(icsp.SALES_DT, -21)
-- MAGIC     AND sles.SALES_DT IN (${last21DateLiilst})
-- MAGIC     --AND icsp.SALES_DT in (${dateList})  
-- MAGIC LEFT JOIN ${Retailer_Client}_retail_alert_im.vw_${recordSourceCode}_forecast_baseline_unit lfbu
-- MAGIC   ON sles.HUB_ORGANIZATION_UNIT_HK = lfbu.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   AND sles.HUB_RETAILER_ITEM_HK = lfbu.HUB_RETAILER_ITEM_HK
-- MAGIC   AND sles.SALES_DT = lfbu.SALES_DT
-- MAGIC GROUP BY
-- MAGIC     sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , sles.HUB_RETAILER_ITEM_HK
-- MAGIC   , icsp.SALES_DT
-- MAGIC HAVING
-- MAGIC   AVG(CASE WHEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0) > nvl(sles.POS_ITEM_QTY, 0)
-- MAGIC           THEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0)
-- MAGIC           ELSE nvl(sles.POS_ITEM_QTY, 0)
-- MAGIC         END) > 0
-- MAGIC """
-- MAGIC val icspwaDF = spark.sql(sqlICSPWAU)
-- MAGIC // Caching just to be onsafer side. If more than one action called on this df then and only then it will be cashed and not re-derived on 2nd action
-- MAGIC icspwaDF.cache
-- MAGIC icspwaDF.createOrReplaceTempView("icspwau")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Final query to derive required data before loading to the target table

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val sql = f"""
-- MAGIC 
-- MAGIC SELECT
-- MAGIC   icsp.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , icsp.HUB_RETAILER_ITEM_HK
-- MAGIC   , current_timestamp() AS LOAD_TS
-- MAGIC   , "" AS ALERT_MESSAGE_DESC
-- MAGIC   , "Inv Cleanup" AS ALERT_TYPE_NM
-- MAGIC   , icsp.ON_HAND_INVENTORY_QTY * icsp.UNIT_PRICE_AMT * 0.9 AS LOST_SALES_AMT
-- MAGIC   , icsp.ON_HAND_INVENTORY_QTY / icspwau.WEEKLY_AVG_POS_ITEM_QTY AS WEEKS_COVER_NUM
-- MAGIC   , icsp.ON_HAND_INVENTORY_QTY
-- MAGIC   , icsp.SALES_DT
-- MAGIC   , "${recordSourceCode}" AS RECORD_SOURCE_CD
-- MAGIC FROM icsp
-- MAGIC 
-- MAGIC INNER JOIN icspwau
-- MAGIC   ON icsp.HUB_ORGANIZATION_UNIT_HK = icspwau.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   AND icsp.HUB_RETAILER_ITEM_HK = icspwau.HUB_RETAILER_ITEM_HK
-- MAGIC   AND icsp.SALES_DT = icspwau.SALES_DT
-- MAGIC 
-- MAGIC LEFT JOIN ${Retailer_Client}_retail_alert_im.alert_inventory_cleanup inv
-- MAGIC   ON icsp.SALES_DT = inv.SALES_DT
-- MAGIC   AND inv.RECORD_SOURCE_CD = "${recordSourceCode}"
-- MAGIC   AND icsp.HUB_ORGANIZATION_UNIT_HK = inv.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   AND icsp.HUB_RETAILER_ITEM_HK = inv.HUB_RETAILER_ITEM_HK
-- MAGIC   AND inv.ALERT_TYPE_NM = "Inv Cleanup"
-- MAGIC   AND CAST(nvl(icsp.ON_HAND_INVENTORY_QTY * icsp.UNIT_PRICE_AMT * 0.9, 0) AS DECIMAL(15,2)) = nvl(inv.LOST_SALES_AMT, 0)
-- MAGIC   AND CAST(icsp.ON_HAND_INVENTORY_QTY / icspwau.WEEKLY_AVG_POS_ITEM_QTY AS DECIMAL(15,2)) = inv.WEEKS_COVER_NUM
-- MAGIC   AND icsp.ON_HAND_INVENTORY_QTY = inv.ON_HAND_INVENTORY_QTY
-- MAGIC   AND inv.SALES_DT in (${dateList})
-- MAGIC 
-- MAGIC WHERE
-- MAGIC   inv.SALES_DT IS NULL
-- MAGIC   """
-- MAGIC 
-- MAGIC val output = spark.sql(sql)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.set("hive.exec.dynamic.partition", "true")
-- MAGIC spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
-- MAGIC spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC // Reducing partitions to 10 here, right before write action. This approach helps execute prior actions with default number of reducers(200), leads to more tasks and hence faster execution
-- MAGIC // and use 10 partitions while writing to table
-- MAGIC 
-- MAGIC // Writing to a temp table to avoid below error:
-- MAGIC //org.apache.spark.sql.AnalysisException: Cannot overwrite a path that is also being read from.
-- MAGIC 
-- MAGIC val tmpPath = f"adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/tmp_alert_inventory_cleanup"
-- MAGIC output.write.mode(SaveMode.Overwrite).format("parquet").save(tmpPath)
-- MAGIC 
-- MAGIC val dfFinal = spark.read.format("parquet").load(tmpPath)
-- MAGIC dfFinal.coalesce(10).write.mode(SaveMode.Append).insertInto(f"${Retailer_Client}_retail_alert_im.alert_inventory_cleanup")
-- MAGIC dbutils.fs.rm(tmpPath, true)

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.alert_inventory_cleanup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Record last LOAD_TS processed

-- COMMAND ----------

INSERT OVERWRITE TABLE ${Retailer_Client}_retail_alert_im.audit_${recordSourceCode}_alerts_invcleanup_last_processed_ts
SELECT
  MAX(LOAD_TS) AS LOAD_TS,
  date_format(current_timestamp, "yyyy-MM-dd hh:mm:ss") AS CREATE_TS
FROM ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_invcleanup_sales_dt;

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.audit_${recordSourceCode}_alerts_invcleanup_last_processed_ts;