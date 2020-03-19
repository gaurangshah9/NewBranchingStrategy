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

-- MAGIC %scala
-- MAGIC val retailerClient = dbutils.widgets.get("Retailer_Client")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Update the latest LOAD_TS consumed in this batch back to AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS
-- MAGIC 
-- MAGIC This updated LOAD_TS will be used by the next batch to identify dates to forecast

-- COMMAND ----------

INSERT OVERWRITE TABLE ${Retailer_Client}_retail_alert_im.AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS
SELECT
  MAX(LOAD_TS) AS LOAD_TS,
  date_format(current_timestamp, "yyyy-MM-dd hh:mm:ss") AS CREATE_TS
FROM
  ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Drop Temp HOLD table

-- COMMAND ----------

DROP TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Refresh table DRFE_FORECAST_BASELINE_UNIT

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.DRFE_FORECAST_BASELINE_UNIT;
