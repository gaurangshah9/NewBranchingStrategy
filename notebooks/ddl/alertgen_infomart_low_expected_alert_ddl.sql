-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("adls_account_name", "", "adls_account_name")
-- MAGIC dbutils.widgets.text("retailer_client", "", "retailer_client")
-- MAGIC dbutils.widgets.text("country_code", "uk", "country_code")
-- MAGIC dbutils.widgets.text("record_source_code", "drfe", "record_source_code")

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC ### alert_osa_low_expected_sale

-- COMMAND ----------


USE ${retailer_client}_${country_code}_retail_alert_im;
DROP TABLE if exists alert_osa_low_expected_sale;
CREATE TABLE if not exists alert_osa_low_expected_sale
  (
      HUB_ORGANIZATION_UNIT_HK STRING,
      HUB_RETAILER_ITEM_HK STRING,
      ORGANIZATION_UNIT_NUM STRING,
      RETAILER_ITEM_ID STRING,
      ON_HAND_INVENTORY_QTY DECIMAL(15,2),
      LOAD_TS TIMESTAMP,
      ALERT_MESSAGE_DESC STRING, 
      ALERT_TYPE_NM STRING,
      LOST_SALES_AMT DECIMAL(15,2),
      SALES_DT DATE,
      RECORD_SOURCE_CD STRING

  )

  USING delta
  PARTITIONED BY (SALES_DT,RECORD_SOURCE_CD)
  LOCATION 'adl://${adls_account_name}.azuredatalakestore.net/informationmart/${retailer_client}_${country_code}_retail_alert/alert_osa_low_expected_sale'


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### vw_alert_osa_low_expected_sale

-- COMMAND ----------

USE ${retailer_client}_${country_code}_retail_alert_im;
create or replace view vw_alert_osa_low_expected_sale  as
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , ORGANIZATION_UNIT_NUM
  , HUB_RETAILER_ITEM_HK
  , RETAILER_ITEM_ID
  , ALERT_MESSAGE_DESC
  , ALERT_TYPE_NM
  , ON_HAND_INVENTORY_QTY
  , LOST_SALES_AMT
  , SALES_DT
  , RECORD_SOURCE_CD
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , ORGANIZATION_UNIT_NUM
        , HUB_RETAILER_ITEM_HK
        , RETAILER_ITEM_ID
        , ALERT_MESSAGE_DESC
        , ALERT_TYPE_NM
        , ON_HAND_INVENTORY_QTY
        , LOST_SALES_AMT
        , SALES_DT
        , RECORD_SOURCE_CD
        , ROW_NUMBER() OVER (PARTITION BY SALES_DT, RECORD_SOURCE_CD, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK ORDER BY LOAD_TS DESC) rnum
      FROM alert_osa_low_expected_sale) z
WHERE
  rnum = 1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### audit_${record_source_code}_alerts_osa_low_expected_sale_last_processed_ts

-- COMMAND ----------

USE ${retailer_client}_${country_code}_retail_alert_im;
CREATE TABLE IF NOT EXISTS audit_${record_source_code}_alerts_osa_low_expected_sale_last_processed_ts
(
    LOAD_TS TIMESTAMP
    ,CREATE_TS TIMESTAMP
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `quote` '"',
  `timestampFormat` 'yyyy-MM-dd HH:mm:ss',
  `escape` '"',
  `header` 'false',
  `delimiter` ',',
  path 'adl://${adls_account_name}.azuredatalakestore.net/informationmart/${retailer_client}_${country_code}_retail_alert/audit_${record_source_code}_alerts_osa_low_expected_sale_last_processed_ts')

-- COMMAND ----------


