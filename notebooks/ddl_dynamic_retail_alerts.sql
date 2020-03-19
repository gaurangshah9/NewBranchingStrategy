-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #Alert Tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_retail_alert_im.alert_on_shelf_availability
( HUB_ORGANIZATION_UNIT_HK STRING
  , HUB_RETAILER_ITEM_HK STRING
  , LOAD_TS TIMESTAMP
  , RECORD_SOURCE_CD STRING
  , ALERT_MESSAGE_DESC STRING
  , ALERT_TYPE_NM STRING
  , ON_HAND_INVENTORY_QTY DECIMAL(15,2)
  , LOST_SALES_AMT DECIMAL(15,2)
  , SALES_DT DATE)
USING org.apache.spark.sql.parquet
OPTIONS (
  `compression` 'snappy',
  `serialization.format` '1',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/alert_on_shelf_availability')
PARTITIONED BY (SALES_DT)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_retail_alert_im.alert_inventory_cleanup
( HUB_ORGANIZATION_UNIT_HK STRING
  , HUB_RETAILER_ITEM_HK STRING
  , LOAD_TS TIMESTAMP
  , RECORD_SOURCE_CD STRING
  , ALERT_MESSAGE_DESC STRING
  , ALERT_TYPE_NM STRING
  , LOST_SALES_AMT DECIMAL(15,2) 
  , WEEKS_COVER_NUM DECIMAL(15,2)
  , ON_HAND_INVENTORY_QTY DECIMAL(15,2)
  , SALES_DT DATE)
USING org.apache.spark.sql.parquet
OPTIONS (
  `compression` 'snappy',
  `serialization.format` '1',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/alert_inventory_cleanup')
PARTITIONED BY (SALES_DT)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_retail_alert_im.audit_alerts_osa_last_processed_ts
( LOAD_TS TIMESTAMP
  , CREATE_TS TIMESTAMP)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `quote` '"',
  `timestampFormat` 'yyyy-MM-dd HH:mm:ss',
  `escape` '"',
  `header` 'false',
  `delimiter` ',',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/audit_alerts_osa_last_processed_ts')


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_retail_alert_im.audit_alerts_invcleanup_last_processed_ts
( LOAD_TS TIMESTAMP
  , CREATE_TS TIMESTAMP)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `quote` '"',
  `timestampFormat` 'yyyy-MM-dd HH:mm:ss',
  `escape` '"',
  `header` 'false',
  `delimiter` ',',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/audit_alerts_invcleanup_last_processed_ts')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_retail_alert_im.tmp_hold_audit_alerts_osa_sales_dt
( SALES_DT DATE
  , LOAD_TS TIMESTAMP)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `quote` '"',
  `timestampFormat` 'yyyy-MM-dd HH:mm:ss',
  `escape` '"',
  `header` 'false',
  `delimiter` ',',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/tmp_hold_audit_alerts_osa_sales_dt')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_retail_alert_im.tmp_hold_audit_alerts_invcleanup_sales_dt
( SALES_DT DATE
  , LOAD_TS TIMESTAMP)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `quote` '"',
  `timestampFormat` 'yyyy-MM-dd HH:mm:ss',
  `escape` '"',
  `header` 'false',
  `delimiter` ',',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/tmp_hold_audit_alerts_invcleanup_sales_dt')

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_retail_alert_im.vw_loess_lost_sales_value
AS
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , HUB_RETAILER_ITEM_HK
  , SALES_DT
  , LOST_SALES_AMT
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , SALES_DT
        , LOST_SALES_AMT
        , ROW_NUMBER() OVER(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK ORDER BY LOAD_TS DESC) rnum
      FROM ${Retailer_Client}_retail_alert_im.loess_lost_sales_value) z
WHERE
  rnum = 1;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_retail_alert_im.vw_loess_forecast_baseline_unit
AS
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , HUB_RETAILER_ITEM_HK
  , SALES_DT
  , BASELINE_POS_ITEM_QTY
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , SALES_DT
        , BASELINE_POS_ITEM_QTY
        , ROW_NUMBER() OVER(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK ORDER BY LOAD_TS DESC) rnum
      FROM ${Retailer_Client}_retail_alert_im.loess_forecast_baseline_unit) z
WHERE
  rnum = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #Dynamic Retail
-- MAGIC ##On-Shelf Availability
-- MAGIC 
-- MAGIC OSA alerts consist of 3 sub-alerts ranked by priority
-- MAGIC 1. Phantom Inventory
-- MAGIC 2. Availability Voids
-- MAGIC 3. Slow Sales
-- MAGIC 
-- MAGIC These sub alerts a exclusive, so a product cannot have more than 1 OSA per store.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### 1. Calculate Phantom Inventory
-- MAGIC Phantom inventory is defined as products that have been forecast to sell 1 or more products for the last 5 consecutive days and has sold 0 products in the same 5 consecutive days.

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_retail_alert_im.vw_osa_phantom_inventory AS
WITH CALC_DATE AS
( SELECT
     ALERT_DT
     , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
   FROM (SELECT
           SALES_DT AS ALERT_DT
           , SALES_DT AS START_SALES_DT
           , DATE_ADD(CAST(SALES_DT AS DATE), -4) AS END_SALES_DT
         FROM ${Retailer_Client}_retail_alert_im.tmp_hold_audit_alerts_osa_sales_dt) TMP
   LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
, SALES_DATA AS
( SELECT
    sles.HUB_ORGANIZATION_UNIT_HK
    , sles.HUB_RETAILER_ITEM_HK
    , msd.ALERT_DT
    , sles.SALES_DT
    , llsv.LOST_SALES_AMT
  FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
  INNER JOIN CALC_DATE msd
    ON sles.SALES_DT = msd.CALC_DT
  INNER JOIN ${Retailer_Client}_retail_alert_im.vw_loess_lost_sales_value llsv
    ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = llsv.SALES_DT
  INNER JOIN ${Retailer_Client}_retail_alert_im.vw_loess_forecast_baseline_unit lfbu
    ON sles.HUB_ORGANIZATION_UNIT_HK = lfbu.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = lfbu.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = lfbu.SALES_DT
  WHERE
    sles.POS_ITEM_QTY = 0
    AND lfbu.BASELINE_POS_ITEM_QTY >= 1)
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , HUB_RETAILER_ITEM_HK
  , "Availability (Phantom Inventory/Book Stock Error)" AS ALERT_MESSAGE_DESC
  , 1 AS ALERT_PRIORITY_ID
  , ALERT_DT AS SALES_DT
  , LOST_SALES_AMT
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , ALERT_DT
        , LOST_SALES_AMT
        , ROW_NUMBER() OVER(PARTITION BY HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, ALERT_DT ORDER BY SALES_DT ASC) AS RNK
      FROM SALES_DATA) z
WHERE
  RNK = 5
  AND LOST_SALES_AMT > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### 2. Calculate Availability Voids
-- MAGIC Availability Voids is defined as products that have been forecast to sell 2 or more products for the last 2 consecutive days and has sold 0 products in the same 2 consecutive days.

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_retail_alert_im.vw_osa_availability_voids AS
WITH CALC_DATE AS
( SELECT
    ALERT_DT
    , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
  FROM (SELECT
          SALES_DT AS ALERT_DT
          , SALES_DT AS START_SALES_DT
          , DATE_ADD(CAST(SALES_DT AS DATE), -1) AS END_SALES_DT
        FROM ${Retailer_Client}_retail_alert_im.tmp_hold_audit_alerts_osa_sales_dt) TMP
  LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
, SALES_DATA AS
( SELECT
    sles.HUB_ORGANIZATION_UNIT_HK
    , sles.HUB_RETAILER_ITEM_HK
    , msd.ALERT_DT
    , sles.SALES_DT
    , llsv.LOST_SALES_AMT
  FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
  INNER JOIN  CALC_DATE msd
    ON sles.SALES_DT = msd.CALC_DT
  INNER JOIN ${Retailer_Client}_retail_alert_im.vw_loess_lost_sales_value llsv
    ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = llsv.SALES_DT
  INNER JOIN ${Retailer_Client}_retail_alert_im.vw_loess_forecast_baseline_unit lfbu
    ON sles.HUB_ORGANIZATION_UNIT_HK = lfbu.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = lfbu.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = lfbu.SALES_DT
  WHERE
    sles.POS_ITEM_QTY = 0
    AND lfbu.BASELINE_POS_ITEM_QTY >= 2)
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , HUB_RETAILER_ITEM_HK
  , "Availability (Voids)" AS ALERT_MESSAGE_DESC
  , 2 AS ALERT_PRIORITY_ID
  , ALERT_DT AS SALES_DT
  , LOST_SALES_AMT
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , ALERT_DT
        , LOST_SALES_AMT
        , ROW_NUMBER() OVER(PARTITION BY HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, ALERT_DT ORDER BY SALES_DT ASC) AS RNK
      FROM SALES_DATA) z
WHERE
  RNK = 2
  AND LOST_SALES_AMT > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### 3. Calculate Slow Sales
-- MAGIC 
-- MAGIC Slow sales is defined as a products having a total LSV value greater than 0 over the last 2 days.

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_retail_alert_im.vw_osa_slow_sales AS
SELECT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , "Slow Sales" AS ALERT_MESSAGE_DESC
  , 3 AS ALERT_PRIORITY_ID
  , adsd.SALES_DT
  , SUM(llsv.LOST_SALES_AMT) AS LOST_SALES_AMT
FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_retail_alert_im.tmp_hold_audit_alerts_osa_sales_dt adsd
  ON sles.SALES_DT <= adsd.SALES_DT
  AND sles.SALES_DT >= DATE_ADD(CAST(adsd.SALES_DT AS DATE), -1)
INNER JOIN ${Retailer_Client}_retail_alert_im.vw_loess_lost_sales_value llsv
  ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
  AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
  AND sles.SALES_DT = llsv.SALES_DT
GROUP BY
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , adsd.SALES_DT
HAVING
  SUM(llsv.LOST_SALES_AMT) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #Dynamic Retail
-- MAGIC ##Inventory Cleanup
-- MAGIC 
-- MAGIC The alerts indicate how many weeks the current inventory level for a given product will take to sell. Agents will build extra displays to help increase the sales of products with high inventory / weeks

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_retail_alert_im.vw_inventory_cleanup_store_product AS
SELECT DISTINCT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , sles.SALES_DT
  , sles.ON_HAND_INVENTORY_QTY
  , slesup.UNIT_PRICE_AMT
FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_retail_alert_im.tmp_hold_audit_alerts_invcleanup_sales_dt adsd
  ON sles.SALES_DT = adsd.SALES_DT
INNER JOIN ${Retailer_Client}_dv.vw_sat_retailer_item_unit_price slesup
  ON sles.HUB_ORGANIZATION_UNIT_HK = slesup.HUB_ORGANIZATION_UNIT_HK
  AND sles.HUB_RETAILER_ITEM_HK = slesup.HUB_RETAILER_ITEM_HK
     AND sles.SALES_DT = slesup.SALES_DT
WHERE
  sles.ON_HAND_INVENTORY_QTY > 0;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_retail_alert_im.vw_inventory_cleanup_store_product_weekly_avg_units AS
SELECT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , icsp.SALES_DT
  , AVG(CASE WHEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0) > nvl(sles.POS_ITEM_QTY, 0)
          THEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0)
          ELSE nvl(sles.POS_ITEM_QTY, 0)
        END) * 7 AS WEEKLY_AVG_POS_ITEM_QTY
FROM ${Retailer_Client}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_retail_alert_im.vw_inventory_cleanup_store_product icsp
    ON sles.HUB_ORGANIZATION_UNIT_HK = icsp.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = icsp.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT <= icsp.SALES_DT
    AND sles.SALES_DT >= DATE_ADD(icsp.SALES_DT, -21)
LEFT JOIN ${Retailer_Client}_retail_alert_im.vw_loess_forecast_baseline_unit lfbu
  ON sles.HUB_ORGANIZATION_UNIT_HK = lfbu.HUB_ORGANIZATION_UNIT_HK
  AND sles.HUB_RETAILER_ITEM_HK = lfbu.HUB_RETAILER_ITEM_HK
  AND sles.SALES_DT = lfbu.SALES_DT
GROUP BY
    sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , icsp.SALES_DT
HAVING
  AVG(CASE WHEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0) > nvl(sles.POS_ITEM_QTY, 0)
          THEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0)
          ELSE nvl(sles.POS_ITEM_QTY, 0)
        END) > 0;