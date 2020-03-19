-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")
-- MAGIC dbutils.widgets.text("recordSourceCode", "", "recordSourceCode")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Read Retailer_Client, Record_Source_Code and ADLS_Account_Name widget values in Scala local variables
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
-- MAGIC This would potentially happen in the very first run where the process expects some value stored in table AUDIT_ALERTS_OSA_LAST_PROCESSED_TS to let the process know which dates to process.

-- COMMAND ----------

SELECT * FROM ${Retailer_Client}_retail_alert_im.audit_${recordSourceCode}_alerts_osa_last_processed_ts;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${Retailer_Client}_retail_alert_im.AUDIT_${recordSourceCode}_ALERTS_OSA_LAST_PROCESSED_TS")
-- MAGIC val count = df.count
-- MAGIC println(count)
-- MAGIC if (count == 0 ){
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Temp hold of SALES_DT and LOAD_TS to process

-- COMMAND ----------

DROP TABLE IF EXISTS ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_osa_sales_dt;
CREATE TABLE ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_osa_sales_dt AS
SELECT
  SALES_DT
  , LOAD_TS
FROM (SELECT
        a.SALES_DT
        , a.LOAD_TS
        , ROW_NUMBER() OVER(PARTITION BY a.SALES_DT ORDER BY a.LOAD_TS DESC) rnum
      FROM ${Retailer_Client}_dv.audit_driver_sales_dates a
      INNER JOIN ${Retailer_Client}_retail_alert_im.audit_${recordSourceCode}_alerts_osa_last_processed_ts b
        ON a.LOAD_TS > b.LOAD_TS) a
WHERE
  rnum = 1;

-- COMMAND ----------

select * from ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_osa_sales_dt;

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_osa_sales_dt;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit the notebook if there is no new dates to process

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_OSA_SALES_DT")
-- MAGIC val count = df.count
-- MAGIC println(count)
-- MAGIC if (count == 0 ){
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Get list of dates from TMP Hold table a string to pass to dynamic SQLs

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val dateList: String = spark.sql(f"SELECT * FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_OSA_SALES_DT").collect.map("'" + _(0) + "'").mkString(",")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Derive vw_${recordSourceCode}_osa_phantom_inventory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Derive Dates needed to be placed as predicates

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val s = f"""
-- MAGIC SELECT
-- MAGIC      ALERT_DT
-- MAGIC      , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
-- MAGIC    FROM (SELECT
-- MAGIC            SALES_DT AS ALERT_DT
-- MAGIC            , SALES_DT AS START_SALES_DT
-- MAGIC            , DATE_ADD(CAST(SALES_DT AS DATE), -4) AS END_SALES_DT
-- MAGIC          FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_OSA_SALES_DT) TMP
-- MAGIC    LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X
-- MAGIC """
-- MAGIC val calcDateList_4: String = spark.sql(s).collect.map("'" + _(1) + "'").distinct.mkString(",")
-- MAGIC val alertDateList: String = spark.sql(s).collect.map("'" + _(0) + "'").distinct.mkString(",")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Get vw_${recordSourceCode}_osa_phantom_inventory i.e. vw_loess/drfe_osa_phantom_inventory

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val sql = f"""
-- MAGIC WITH CALC_DATE AS
-- MAGIC ( SELECT
-- MAGIC      ALERT_DT
-- MAGIC      , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
-- MAGIC    FROM (SELECT
-- MAGIC            SALES_DT AS ALERT_DT
-- MAGIC            , SALES_DT AS START_SALES_DT
-- MAGIC            , DATE_ADD(CAST(SALES_DT AS DATE), -4) AS END_SALES_DT
-- MAGIC          FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_OSA_SALES_DT) TMP
-- MAGIC    LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
-- MAGIC , SALES_DATA AS
-- MAGIC ( SELECT
-- MAGIC     sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC     , sles.HUB_RETAILER_ITEM_HK
-- MAGIC     , msd.ALERT_DT
-- MAGIC     , sles.SALES_DT
-- MAGIC     , llsv.LOST_SALES_AMT
-- MAGIC   FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
-- MAGIC   INNER JOIN CALC_DATE msd
-- MAGIC     ON sles.SALES_DT = msd.CALC_DT
-- MAGIC     AND msd.CALC_DT IN (${calcDateList_4})
-- MAGIC   INNER JOIN (
-- MAGIC               select * 
-- MAGIC               from ${Retailer_Client}_retail_alert_im.vw_lost_sales_value 
-- MAGIC               where 
-- MAGIC                 SALES_DT IN (${calcDateList_4}) AND
-- MAGIC                 UPPER(RECORD_SOURCE_CD) = UPPER('${recordSourceCode}')
-- MAGIC               ) AS llsv
-- MAGIC     ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
-- MAGIC     AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
-- MAGIC     AND sles.SALES_DT = llsv.SALES_DT
-- MAGIC   INNER JOIN ${Retailer_Client}_retail_alert_im.vw_${recordSourceCode}_forecast_baseline_unit lfbu
-- MAGIC     ON sles.HUB_ORGANIZATION_UNIT_HK = lfbu.HUB_ORGANIZATION_UNIT_HK
-- MAGIC     AND sles.HUB_RETAILER_ITEM_HK = lfbu.HUB_RETAILER_ITEM_HK
-- MAGIC     AND sles.SALES_DT = lfbu.SALES_DT
-- MAGIC     AND lfbu.SALES_DT IN (${calcDateList_4})
-- MAGIC   WHERE
-- MAGIC     sles.ON_HAND_INVENTORY_QTY > 0
-- MAGIC     AND sles.POS_ITEM_QTY = 0
-- MAGIC     AND lfbu.BASELINE_POS_ITEM_QTY >= 1)
-- MAGIC SELECT
-- MAGIC   HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , HUB_RETAILER_ITEM_HK
-- MAGIC   , "Availability (Phantom Inventory/Book Stock Error)" AS ALERT_MESSAGE_DESC
-- MAGIC   , 1 AS ALERT_PRIORITY_ID
-- MAGIC   , ALERT_DT AS SALES_DT
-- MAGIC   , LOST_SALES_AMT
-- MAGIC FROM (SELECT
-- MAGIC         HUB_ORGANIZATION_UNIT_HK
-- MAGIC         , HUB_RETAILER_ITEM_HK
-- MAGIC         , ALERT_DT
-- MAGIC         , LOST_SALES_AMT
-- MAGIC         , ROW_NUMBER() OVER(PARTITION BY HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, ALERT_DT ORDER BY SALES_DT ASC) AS RNK
-- MAGIC       FROM SALES_DATA) z
-- MAGIC WHERE
-- MAGIC   RNK = 5
-- MAGIC   AND LOST_SALES_AMT > 0"""
-- MAGIC 
-- MAGIC val vw_osa_phantom_inventoryDF = spark.sql(sql)
-- MAGIC 
-- MAGIC // Caching just to be onsafer side. If more than one action called on this df then and only then it will be cashed and not re-derived on 2nd action
-- MAGIC vw_osa_phantom_inventoryDF.cache
-- MAGIC vw_osa_phantom_inventoryDF.createOrReplaceTempView("vw_"+dbutils.widgets.get("recordSourceCode")+"_osa_phantom_inventory")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Derive vw_${recordSourceCode}_osa_availability_voids i.e. vw_loess/drfe_osa_availability_voids

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Derive Dates needed to be placed as predicates

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val s = f"""
-- MAGIC SELECT
-- MAGIC     ALERT_DT
-- MAGIC     , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
-- MAGIC   FROM (SELECT
-- MAGIC           SALES_DT AS ALERT_DT
-- MAGIC           , SALES_DT AS START_SALES_DT
-- MAGIC           , DATE_ADD(CAST(SALES_DT AS DATE), -1) AS END_SALES_DT
-- MAGIC         FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_OSA_SALES_DT) TMP
-- MAGIC   LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X
-- MAGIC """
-- MAGIC val calcDateList_1: String = spark.sql(s).collect.map("'" + _(1) + "'").distinct.mkString(",")
-- MAGIC val alertDateList: String = spark.sql(s).collect.map("'" + _(0) + "'").distinct.mkString(",")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Get vw_${recordSourceCode}_osa_availability_voids i.e. vw_loess/drfe_osa_availability_voids

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val sql = f"""
-- MAGIC WITH CALC_DATE AS
-- MAGIC ( SELECT
-- MAGIC     ALERT_DT
-- MAGIC     , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
-- MAGIC   FROM (SELECT
-- MAGIC           SALES_DT AS ALERT_DT
-- MAGIC           , SALES_DT AS START_SALES_DT
-- MAGIC           , DATE_ADD(CAST(SALES_DT AS DATE), -1) AS END_SALES_DT
-- MAGIC         FROM ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_OSA_SALES_DT) TMP
-- MAGIC   LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
-- MAGIC , SALES_DATA AS
-- MAGIC ( SELECT
-- MAGIC     sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC     , sles.HUB_RETAILER_ITEM_HK
-- MAGIC     , msd.ALERT_DT
-- MAGIC     , sles.SALES_DT
-- MAGIC     , llsv.LOST_SALES_AMT
-- MAGIC   FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
-- MAGIC   INNER JOIN  CALC_DATE msd
-- MAGIC     ON sles.SALES_DT = msd.CALC_DT
-- MAGIC     AND msd.CALC_DT IN (${calcDateList_1})
-- MAGIC   INNER JOIN (
-- MAGIC               select * 
-- MAGIC               from ${Retailer_Client}_retail_alert_im.vw_lost_sales_value 
-- MAGIC               where
-- MAGIC                 UPPER(RECORD_SOURCE_CD) = UPPER('${recordSourceCode}')
-- MAGIC                 AND SALES_DT IN (${calcDateList_1})
-- MAGIC              ) llsv
-- MAGIC     ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
-- MAGIC     AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
-- MAGIC     AND sles.SALES_DT = llsv.SALES_DT
-- MAGIC   INNER JOIN ${Retailer_Client}_retail_alert_im.vw_${recordSourceCode}_forecast_baseline_unit lfbu
-- MAGIC     ON sles.HUB_ORGANIZATION_UNIT_HK = lfbu.HUB_ORGANIZATION_UNIT_HK
-- MAGIC     AND sles.HUB_RETAILER_ITEM_HK = lfbu.HUB_RETAILER_ITEM_HK
-- MAGIC     AND sles.SALES_DT = lfbu.SALES_DT
-- MAGIC     AND lfbu.SALES_DT IN (${calcDateList_1})
-- MAGIC   WHERE
-- MAGIC     sles.POS_ITEM_QTY = 0
-- MAGIC     AND lfbu.BASELINE_POS_ITEM_QTY >= 2)
-- MAGIC SELECT
-- MAGIC   HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , HUB_RETAILER_ITEM_HK
-- MAGIC   , "Availability (Voids)" AS ALERT_MESSAGE_DESC
-- MAGIC   , 2 AS ALERT_PRIORITY_ID
-- MAGIC   , ALERT_DT AS SALES_DT
-- MAGIC   , LOST_SALES_AMT
-- MAGIC FROM (SELECT
-- MAGIC         HUB_ORGANIZATION_UNIT_HK
-- MAGIC         , HUB_RETAILER_ITEM_HK
-- MAGIC         , ALERT_DT
-- MAGIC         , LOST_SALES_AMT
-- MAGIC         , ROW_NUMBER() OVER(PARTITION BY HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, ALERT_DT ORDER BY SALES_DT ASC) AS RNK
-- MAGIC       FROM SALES_DATA) z
-- MAGIC WHERE
-- MAGIC   RNK = 2
-- MAGIC   AND LOST_SALES_AMT > 0"""
-- MAGIC 
-- MAGIC val vw_osa_availability_voidsDF = spark.sql(sql)
-- MAGIC 
-- MAGIC // Caching just to be onsafer side. If more than one action called on this df then and only then it will be cashed and not re-derived on 2nd action
-- MAGIC vw_osa_availability_voidsDF.cache
-- MAGIC vw_osa_availability_voidsDF.createOrReplaceTempView("vw_"+dbutils.widgets.get("recordSourceCode")+"_osa_availability_voids")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Derive vw_${recordSourceCode}_osa_slow_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Get vw_${recordSourceCode}_osa_slow_sales i.e. vw loess/drfe_osa_slow_sales

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val sql = f"""
-- MAGIC SELECT
-- MAGIC   sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , sles.HUB_RETAILER_ITEM_HK
-- MAGIC   , "Slow Sales" AS ALERT_MESSAGE_DESC
-- MAGIC   , 3 AS ALERT_PRIORITY_ID
-- MAGIC   , adsd.SALES_DT
-- MAGIC   , SUM(llsv.LOST_SALES_AMT) AS LOST_SALES_AMT
-- MAGIC FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
-- MAGIC INNER JOIN ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_${recordSourceCode}_ALERTS_OSA_SALES_DT adsd
-- MAGIC   ON sles.SALES_DT <= adsd.SALES_DT
-- MAGIC   AND sles.SALES_DT >= DATE_ADD(CAST(adsd.SALES_DT AS DATE), -1)
-- MAGIC   AND adsd.SALES_DT IN (${dateList})
-- MAGIC   AND sles.SALES_DT IN (${calcDateList_1})
-- MAGIC INNER JOIN (select * from ${Retailer_Client}_retail_alert_im.vw_lost_sales_value where UPPER(RECORD_SOURCE_CD) = UPPER('${recordSourceCode}')) llsv
-- MAGIC   ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
-- MAGIC   AND sles.SALES_DT = llsv.SALES_DT
-- MAGIC GROUP BY
-- MAGIC   sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , sles.HUB_RETAILER_ITEM_HK
-- MAGIC   , adsd.SALES_DT
-- MAGIC HAVING
-- MAGIC   SUM(llsv.LOST_SALES_AMT) > 0
-- MAGIC """
-- MAGIC 
-- MAGIC val vw_osa_slow_salesDF = spark.sql(sql)
-- MAGIC 
-- MAGIC // Caching just to be onsafer side. If more than one action called on this df then and only then it will be cashed and not re-derived on 2nd action
-- MAGIC vw_osa_slow_salesDF.cache
-- MAGIC vw_osa_slow_salesDF.createOrReplaceTempView("vw_"+dbutils.widgets.get("recordSourceCode")+"_osa_slow_sales")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Union all 3 views and assign alert priority
-- MAGIC - vw_${recordSourceCode}_osa_phantom_inventory
-- MAGIC - vw_${recordSourceCode}_osa_availability_voids
-- MAGIC - vw_${recordSourceCode}_osa_slow_sales

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val sql = f"""
-- MAGIC SELECT
-- MAGIC         a.HUB_ORGANIZATION_UNIT_HK
-- MAGIC         , a.HUB_RETAILER_ITEM_HK
-- MAGIC         , a.ALERT_MESSAGE_DESC
-- MAGIC         , a.LOST_SALES_AMT
-- MAGIC         , a.SALES_DT
-- MAGIC       FROM (SELECT
-- MAGIC               a.HUB_ORGANIZATION_UNIT_HK
-- MAGIC               , a.HUB_RETAILER_ITEM_HK
-- MAGIC               , a.ALERT_MESSAGE_DESC
-- MAGIC               , a.ALERT_PRIORITY_ID
-- MAGIC               , a.SALES_DT
-- MAGIC               , a.LOST_SALES_AMT
-- MAGIC               , ROW_NUMBER() OVER(PARTITION BY
-- MAGIC                                     a.HUB_ORGANIZATION_UNIT_HK
-- MAGIC                                     , a.HUB_RETAILER_ITEM_HK
-- MAGIC                                     , a.SALES_DT
-- MAGIC                                   ORDER BY
-- MAGIC                                     a.ALERT_PRIORITY_ID ASC) AS RNK
-- MAGIC             FROM (
-- MAGIC                   SELECT
-- MAGIC                     HUB_ORGANIZATION_UNIT_HK
-- MAGIC                     , HUB_RETAILER_ITEM_HK
-- MAGIC                     , ALERT_MESSAGE_DESC
-- MAGIC                     , ALERT_PRIORITY_ID
-- MAGIC                     , SALES_DT
-- MAGIC                     , LOST_SALES_AMT
-- MAGIC                   FROM vw_${recordSourceCode}_osa_phantom_inventory
-- MAGIC                   UNION ALL
-- MAGIC                   SELECT
-- MAGIC                     HUB_ORGANIZATION_UNIT_HK
-- MAGIC                     , HUB_RETAILER_ITEM_HK
-- MAGIC                     , ALERT_MESSAGE_DESC
-- MAGIC                     , ALERT_PRIORITY_ID
-- MAGIC                     , SALES_DT
-- MAGIC                     , LOST_SALES_AMT
-- MAGIC                   FROM vw_${recordSourceCode}_osa_availability_voids
-- MAGIC                   UNION ALL
-- MAGIC                   SELECT
-- MAGIC                     HUB_ORGANIZATION_UNIT_HK
-- MAGIC                     , HUB_RETAILER_ITEM_HK
-- MAGIC                     , ALERT_MESSAGE_DESC
-- MAGIC                     , ALERT_PRIORITY_ID
-- MAGIC                     , SALES_DT
-- MAGIC                     , LOST_SALES_AMT
-- MAGIC                   FROM vw_${recordSourceCode}_osa_slow_sales) a) a
-- MAGIC       WHERE
-- MAGIC         a.RNK = 1
-- MAGIC """
-- MAGIC val unionAlertPriorityDF = spark.sql(sql)
-- MAGIC 
-- MAGIC // Caching just to be onsafer side. If more than one action called on this df then and only then it will be cashed and not re-derived on 2nd action
-- MAGIC unionAlertPriorityDF.cache
-- MAGIC unionAlertPriorityDF.createOrReplaceTempView("unionAlertPriority")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Get unique sales dates from unionAlertPriority
-- MAGIC 
-- MAGIC This would be the first action in the lazy execution and then the Dataframe will be cached. It will be used without DAG execution for subsequent actions!!

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val salesDtList: String = spark.sql("SELECT DISTINCT SALES_DT FROM unionAlertPriority").collect.map("'" + _(0) + "'").mkString(",")
-- MAGIC 
-- MAGIC if (salesDtList.trim.equals("") ){
-- MAGIC 
-- MAGIC   println(f"No data found in sales, forecast or lsv for the dates ${dateList}")
-- MAGIC   println("Exiting notebook...")
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Final Load Query

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val finalSQL = f"""
-- MAGIC SELECT
-- MAGIC   a.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   , a.HUB_RETAILER_ITEM_HK
-- MAGIC   , current_timestamp() AS LOAD_TS
-- MAGIC   , a.ALERT_MESSAGE_DESC
-- MAGIC   , "OnShelfAvailability" AS ALERT_TYPE_NM
-- MAGIC   , sles.ON_HAND_INVENTORY_QTY
-- MAGIC   , a.LOST_SALES_AMT
-- MAGIC   , a.SALES_DT
-- MAGIC   , "${recordSourceCode}" AS RECORD_SOURCE_CD
-- MAGIC FROM unionAlertPriority a
-- MAGIC INNER JOIN ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
-- MAGIC   ON a.HUB_ORGANIZATION_UNIT_HK = sles.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   AND a.HUB_RETAILER_ITEM_HK = sles.HUB_RETAILER_ITEM_HK
-- MAGIC   AND a.SALES_DT = sles.SALES_DT
-- MAGIC   and A.SALES_DT IN (${salesDtList})
-- MAGIC LEFT JOIN ${Retailer_Client}_retail_alert_im.alert_on_shelf_availability osa
-- MAGIC   ON a.SALES_DT = osa.SALES_DT
-- MAGIC   AND osa.RECORD_SOURCE_CD = "${recordSourceCode}"
-- MAGIC   AND a.HUB_ORGANIZATION_UNIT_HK = osa.HUB_ORGANIZATION_UNIT_HK
-- MAGIC   AND a.HUB_RETAILER_ITEM_HK = osa.HUB_RETAILER_ITEM_HK
-- MAGIC   AND a.ALERT_MESSAGE_DESC = osa.ALERT_MESSAGE_DESC
-- MAGIC   AND osa.ALERT_TYPE_NM = "OnShelfAvailability"
-- MAGIC   AND sles.ON_HAND_INVENTORY_QTY = osa.ON_HAND_INVENTORY_QTY
-- MAGIC   AND a.LOST_SALES_AMT = osa.LOST_SALES_AMT
-- MAGIC   AND osa.SALES_DT IN (${salesDtList})
-- MAGIC WHERE
-- MAGIC   osa.SALES_DT IS NULL
-- MAGIC """  
-- MAGIC 
-- MAGIC val output = spark.sql(finalSQL)
-- MAGIC output.cache
-- MAGIC //output.count

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.set("hive.exec.dynamic.partition", "true")
-- MAGIC spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
-- MAGIC spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
-- MAGIC spark.conf.set("set spark.sql.shuffle.partitions", 10)
-- MAGIC 
-- MAGIC // Reducing partitions to 10 here, right before write action. This approach helps execute prior actions with default number of reducers(200), leads to more tasks and hence faster execution
-- MAGIC // and use 10 partitions while writing to table
-- MAGIC 
-- MAGIC // Writing to a temp table to avoid below error:
-- MAGIC //org.apache.spark.sql.AnalysisException: Cannot overwrite a path that is also being read from.
-- MAGIC 
-- MAGIC val tmpPath = f"adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/tmp_alert_on_shelf_availability"
-- MAGIC output.write.mode(SaveMode.Overwrite).format("parquet").save(tmpPath)
-- MAGIC val dfFinal = spark.read.format("parquet").load(tmpPath)
-- MAGIC dfFinal.coalesce(10).write.mode(SaveMode.Append).insertInto(f"${Retailer_Client}_retail_alert_im.alert_on_shelf_availability")
-- MAGIC dbutils.fs.rm(tmpPath, true)

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.alert_on_shelf_availability;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Record last LOAD_TS processed

-- COMMAND ----------

INSERT OVERWRITE TABLE ${Retailer_Client}_retail_alert_im.audit_${recordSourceCode}_alerts_osa_last_processed_ts
SELECT
  MAX(LOAD_TS) AS LOAD_TS,
  date_format(current_timestamp, "yyyy-MM-dd hh:mm:ss") AS CREATE_TS
FROM ${Retailer_Client}_retail_alert_im.tmp_hold_audit_${recordSourceCode}_alerts_osa_sales_dt;

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.audit_${recordSourceCode}_alerts_osa_last_processed_ts;
