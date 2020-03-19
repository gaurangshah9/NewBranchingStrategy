-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("adls_account_name", "", "adls_account_name")
-- MAGIC dbutils.widgets.text("retailer_client", "", "retailer_client")
-- MAGIC dbutils.widgets.text("country_code", "uk", "country_code")
-- MAGIC dbutils.widgets.text("record_source_code", "drfe", "record_source_code")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### alert_osa_low_expected_sale_grouping DDL 

-- COMMAND ----------

USE ${retailer_client}_${country_code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS alert_osa_low_expected_sale_grouping
(
  ORGANIZATION_UNIT_NUM           INT,
  RETAILER_ITEM_ID                STRING,
  HUB_ORGANIZATION_UNIT_HK        STRING,
  HUB_RETAILER_ITEM_HK            STRING,
  MINIMUM_POS_ITEM_QTY            INT,
  MID_THRESHOLD_QTY               DOUBLE,
  TOP_THRESHOLD_QTY               DOUBLE,
  LOAD_TS                         TIMESTAMP,
  DAYS_IN_GROUP_NUM               INT,
  MEMBERSHIP_THRESHOLD_NUM        INT,
  RECORD_SOURCE_CODE              STRING,
  GROUP_NM                        STRING
)
USING delta 
PARTITIONED BY (RECORD_SOURCE_CODE, GROUP_NM)
LOCATION 'adl://${adls_account_name}.azuredatalakestore.net/informationmart/${retailer_client}_${country_code}_retail_alert/alert_osa_low_expected_sale_grouping'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### alert_osa_invalid_alert_grouping

-- COMMAND ----------

USE ${retailer_client}_${country_code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS alert_osa_invalid_alert_grouping
(
  ORGANIZATION_UNIT_NUM           INT,
  RETAILER_ITEM_ID                STRING,
  HUB_ORGANIZATION_UNIT_HK        STRING,
  HUB_RETAILER_ITEM_HK            STRING,
  MINIMUM_POS_ITEM_QTY            INT,
  MID_THRESHOLD_QTY               DOUBLE,
  TOP_THRESHOLD_QTY               DOUBLE,
  LOAD_TS                         TIMESTAMP,
  DAYS_IN_GROUP_NUM               INT,
  MEMBERSHIP_THRESHOLD_NUM        INT,
  RECORD_SOURCE_CODE              STRING,
  GROUP_NM                        STRING
)
USING delta 
PARTITIONED BY (RECORD_SOURCE_CODE, GROUP_NM)
LOCATION 'adl://${adls_account_name}.azuredatalakestore.net/informationmart/${retailer_client}_${country_code}_retail_alert/alert_osa_invalid_alert_grouping'
