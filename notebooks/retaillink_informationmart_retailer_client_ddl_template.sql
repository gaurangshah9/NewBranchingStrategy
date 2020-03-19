-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Retailer_Client")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Country_Code", "", "Country_Code")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Country_Code")

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
-- MAGIC ## Drop All the objects if the Drop_Create_All_Y_N_Flag is set to Y

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("DROP DATABASE IF EXISTS " + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert_im CASCADE")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Create Retailer_Client Level Information Mart Objects

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_loess_forecast_last_processed_ts")
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_drfe_forecast_last_processed_ts")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS
(
  LOAD_TS TIMESTAMP,
  CREATE_TS TIMESTAMP
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` ',',
  `timestampFormat` "yyyy-MM-dd HH:mm:ss",
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_loess_forecast_last_processed_ts'
);

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS AUDIT_DRFE_FORECAST_LAST_PROCESSED_TS
(
  LOAD_TS TIMESTAMP,
  CREATE_TS TIMESTAMP
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` ',',
  `timestampFormat` "yyyy-MM-dd HH:mm:ss",
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_drfe_forecast_last_processed_ts'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_loess_lost_sales_last_processed_ts")
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_drfe_lost_sales_last_processed_ts")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;
CREATE TABLE IF NOT EXISTS AUDIT_LOESS_LOST_SALES_LAST_PROCESSED_TS
(
  LOAD_TS TIMESTAMP,
  CREATE_TS TIMESTAMP
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` ',',
  `timestampFormat` "yyyy-MM-dd HH:mm:ss",
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_loess_lost_sales_last_processed_ts'
);

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;
CREATE TABLE IF NOT EXISTS AUDIT_DRFE_LOST_SALES_LAST_PROCESSED_TS
(
  LOAD_TS TIMESTAMP,
  CREATE_TS TIMESTAMP
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` ',',
  `timestampFormat` "yyyy-MM-dd HH:mm:ss",
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_drfe_lost_sales_last_processed_ts'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/lost_sales_value")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS LOST_SALES_VALUE
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    LOST_SALES_AMT DECIMAL(15,2),
    SALES_DT DATE,
    RECORD_SOURCE_CD STRING
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT,RECORD_SOURCE_CD)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/lost_sales_value');

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert_im.LOST_SALES_VALUE")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/tmp_loess_forecast_baseline_unit_future_sales_data")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    SALES_DT DATE
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/tmp_loess_forecast_baseline_unit_future_sales_data');

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/tmp_loess_forecast_baseline_unit_history_sales_data")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA
(
    CURRENT_SALES_DT DATE,
    HIST_POS_ITEM_QTY DECIMAL(15,2),
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    HISTORY_SALES_DT DATE
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (HISTORY_SALES_DT)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/tmp_loess_forecast_baseline_unit_history_sales_data');

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;
CREATE OR REPLACE VIEW VW_LOESS_FORECAST_BASELINE_UNIT_TRICUBE_WEIGHT AS
WITH DATA_WITH_DISTANCE_IN_WEEK AS
(
SELECT
  CURRENT_SALES_DT,
  HISTORY_SALES_DT,
  HIST_POS_Item_QTY,
  HUB_Organization_Unit_HK,
  HUB_Retailer_Item_HK,
  ROW_NUMBER() OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK ORDER BY HISTORY_SALES_DT DESC) DISTANCE_IN_WEEK
FROM TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA
)

SELECT
  CURRENT_SALES_DT,
  HUB_Organization_Unit_HK,
  HUB_Retailer_Item_HK,
  DISTANCE_IN_WEEK,
  X_Axis,
  Y_Axis,
  Max_DISTANCE_IN_WEEK,
  STORE_PROD_ROW_COUNT,
  CASE WHEN STORE_PROD_ROW_COUNT >= 2 THEN POWER(1 - POWER(DISTANCE_IN_WEEK / Max_DISTANCE_IN_WEEK, 3), 3) ELSE NULL END AS TRICUBE_WEIGHT
FROM
(
  SELECT
    CURRENT_SALES_DT,
    HISTORY_SALES_DT,
    HIST_POS_Item_QTY,
    HUB_Organization_Unit_HK,
    HUB_Retailer_Item_HK,
    DISTANCE_IN_WEEK,
    ROW_NUMBER() OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK ORDER BY DISTANCE_IN_WEEK DESC) X_Axis,
    COALESCE(HIST_POS_Item_QTY, 0) Y_Axis,
    MAX(DISTANCE_IN_WEEK) OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK) Max_DISTANCE_IN_WEEK,
    COUNT(*) OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK) STORE_PROD_ROW_COUNT
  FROM
    DATA_WITH_DISTANCE_IN_WEEK
  WHERE
    DISTANCE_IN_WEEK <= 12
)X;

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;
CREATE OR REPLACE VIEW VW_GET_LOESS_FORECAST_BASELINE_UNIT AS
SELECT
            HUB_Organization_Unit_HK,
            HUB_Retailer_Item_HK,  
            CASE WHEN (M_SLOPE * (STORE_PROD_ROW_COUNT + 1)) + C_Y_Axis_Intercept < 0 THEN 0 ELSE (M_SLOPE * (STORE_PROD_ROW_COUNT + 1) + C_Y_Axis_Intercept) END AS Y_Axis_Forecasted,
            STORE_PROD_ROW_COUNT,
            M_SLOPE,
            C_Y_Axis_Intercept,
            SALES_DT            
            FROM
            (
                SELECT DISTINCT
                    SALES_DT,
                    HUB_Organization_Unit_HK,
                    HUB_Retailer_Item_HK,  
                    --DISTANCE_IN_WEEK,
                    STORE_PROD_ROW_COUNT,
                    SUM_TRICUBE_WEIGHT,
                    SUM_TRICUBE_WEIGHT_X_Axis,
                    SUM_TRICUBE_WEIGHT_X_Axis_Squared,
                    SUM_TRICUBE_WEIGHT_Y_Axis,
                    SUM_TRICUBE_WEIGHT_X_Axis_Y_Axis,
                    CASE WHEN ((SUM_TRICUBE_WEIGHT * SUM_TRICUBE_WEIGHT_X_Axis_Squared) - POWER(SUM_TRICUBE_WEIGHT_X_Axis, 2)) > 0 THEN ((SUM_TRICUBE_WEIGHT * SUM_TRICUBE_WEIGHT_X_Axis_Y_Axis) - (SUM_TRICUBE_WEIGHT_X_Axis * SUM_TRICUBE_WEIGHT_Y_Axis)) / ((SUM_TRICUBE_WEIGHT * SUM_TRICUBE_WEIGHT_X_Axis_Squared) - POWER(SUM_TRICUBE_WEIGHT_X_Axis, 2)) ELSE NULL END AS M_SLOPE,
                    
                    CASE WHEN ((SUM_TRICUBE_WEIGHT * SUM_TRICUBE_WEIGHT_X_Axis_Squared) - POWER(SUM_TRICUBE_WEIGHT_X_Axis, 2)) > 0 THEN ((SUM_TRICUBE_WEIGHT_X_Axis_Squared * SUM_TRICUBE_WEIGHT_Y_Axis) - (SUM_TRICUBE_WEIGHT_X_Axis * SUM_TRICUBE_WEIGHT_X_Axis_Y_Axis)) / ((SUM_TRICUBE_WEIGHT * SUM_TRICUBE_WEIGHT_X_Axis_Squared) - POWER(SUM_TRICUBE_WEIGHT_X_Axis, 2)) ELSE NULL END AS C_Y_Axis_Intercept
                  FROM
                  (
                    SELECT
                      CURRENT_SALES_DT AS SALES_DT,
                      HUB_Organization_Unit_HK,
                      HUB_Retailer_Item_HK,     
                      DISTANCE_IN_WEEK,
                      STORE_PROD_ROW_COUNT,
                      CASE WHEN STORE_PROD_ROW_COUNT >= 2 THEN SUM(TRICUBE_WEIGHT) OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK) ELSE NULL END AS SUM_TRICUBE_WEIGHT,
                      CASE WHEN STORE_PROD_ROW_COUNT >= 2 THEN SUM(X_Axis * TRICUBE_WEIGHT) OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK) ELSE NULL END AS SUM_TRICUBE_WEIGHT_X_Axis,
                      CASE WHEN STORE_PROD_ROW_COUNT >= 2 THEN SUM(POWER(X_Axis, 2) * TRICUBE_WEIGHT) OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK) ELSE NULL END AS SUM_TRICUBE_WEIGHT_X_Axis_Squared,
                      CASE WHEN STORE_PROD_ROW_COUNT >= 2 THEN SUM(Y_Axis * TRICUBE_WEIGHT) OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK) ELSE NULL END AS SUM_TRICUBE_WEIGHT_Y_Axis,
                      CASE WHEN STORE_PROD_ROW_COUNT >= 2 THEN SUM(X_Axis * Y_Axis * TRICUBE_WEIGHT) OVER(PARTITION BY HUB_Organization_Unit_HK, HUB_Retailer_Item_HK) ELSE NULL END AS SUM_TRICUBE_WEIGHT_X_Axis_Y_Axis
                    FROM VW_LOESS_FORECAST_BASELINE_UNIT_TRICUBE_WEIGHT
                  )x
            );

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/loess_forecast_baseline_unit")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/drfe_forecast_baseline_unit")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS LOESS_FORECAST_BASELINE_UNIT
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    BASELINE_POS_ITEM_QTY DECIMAL(15,2),
    SALES_DT DATE
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/loess_forecast_baseline_unit');

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS DRFE_FORECAST_BASELINE_UNIT
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    BASELINE_POS_ITEM_QTY DECIMAL(15,2),
    MODEL_ID STRING,
    SALES_DT DATE    
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/drfe_forecast_baseline_unit');

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert_im.LOESS_FORECAST_BASELINE_UNIT")
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert_im.DRFE_FORECAST_BASELINE_UNIT")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/alert_on_shelf_availability")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS ALERT_ON_SHELF_AVAILABILITY
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    ALERT_MESSAGE_DESC STRING,
    ALERT_TYPE_NM STRING,
    ON_HAND_INVENTORY_QTY DECIMAL(15,2),
    LOST_SALES_AMT DECIMAL(15,2),
    SALES_DT DATE,
    RECORD_SOURCE_CD STRING
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT, RECORD_SOURCE_CD)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/alert_on_shelf_availability');

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert_im.ALERT_ON_SHELF_AVAILABILITY")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/alert_inventory_cleanup")

-- COMMAND ----------

USE ${Retailer_Client}_${Country_Code}_retail_alert_im;

CREATE TABLE IF NOT EXISTS ALERT_INVENTORY_CLEANUP
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    ALERT_MESSAGE_DESC STRING,
    ALERT_TYPE_NM STRING,
    LOST_SALES_AMT DECIMAL(15,2),
    WEEKS_COVER_NUM DECIMAL(15,2),
    ON_HAND_INVENTORY_QTY DECIMAL(15,2),
    SALES_DT DATE,
    RECORD_SOURCE_CD STRING
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT, RECORD_SOURCE_CD)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/alert_inventory_cleanup');

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert_im.ALERT_INVENTORY_CLEANUP")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_loess_alerts_osa_last_processed_ts")
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_drfe_alerts_osa_last_processed_ts")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.audit_loess_alerts_osa_last_processed_ts
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
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_loess_alerts_osa_last_processed_ts')


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.audit_drfe_alerts_osa_last_processed_ts
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
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_drfe_alerts_osa_last_processed_ts')

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_loess_alerts_invcleanup_last_processed_ts")
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") +"_"+ dbutils.widgets.get("Country_Code") + "_retail_alert/audit_drfe_alerts_invcleanup_last_processed_ts")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.audit_loess_alerts_invcleanup_last_processed_ts
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
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_loess_alerts_invcleanup_last_processed_ts')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.audit_drfe_alerts_invcleanup_last_processed_ts
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
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${Country_Code}_retail_alert/audit_drfe_alerts_invcleanup_last_processed_ts')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_loess_alerts_osa_sales_dt
(SALES_DT DATE, LOAD_TS TIMESTAMP)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_drfe_alerts_osa_sales_dt
(SALES_DT DATE, LOAD_TS TIMESTAMP)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_loess_alerts_invcleanup_sales_dt
( SALES_DT DATE, LOAD_TS TIMESTAMP)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_drfe_alerts_invcleanup_sales_dt
( SALES_DT DATE, LOAD_TS TIMESTAMP)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_lost_sales_value
AS
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , HUB_RETAILER_ITEM_HK
  , SALES_DT
  , RECORD_SOURCE_CD
  , LOST_SALES_AMT
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , SALES_DT
        , RECORD_SOURCE_CD
        , LOST_SALES_AMT
        , ROW_NUMBER() OVER(PARTITION BY SALES_DT, RECORD_SOURCE_CD, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK ORDER BY LOAD_TS DESC) rnum
      FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.lost_sales_value) z
WHERE
  rnum = 1;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_forecast_baseline_unit
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
      FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.loess_forecast_baseline_unit) z
WHERE
  rnum = 1;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_forecast_baseline_unit
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
      FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.drfe_forecast_baseline_unit) z
WHERE
  rnum = 1;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_osa_phantom_inventory AS
WITH CALC_DATE AS
( SELECT
     ALERT_DT
     , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
   FROM (SELECT
           SALES_DT AS ALERT_DT
           , SALES_DT AS START_SALES_DT
           , DATE_ADD(CAST(SALES_DT AS DATE), -4) AS END_SALES_DT
         FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_loess_alerts_osa_sales_dt) TMP
   LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
, SALES_DATA AS
( SELECT
    sles.HUB_ORGANIZATION_UNIT_HK
    , sles.HUB_RETAILER_ITEM_HK
    , msd.ALERT_DT
    , sles.SALES_DT
    , llsv.LOST_SALES_AMT
  FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
  INNER JOIN CALC_DATE msd
    ON sles.SALES_DT = msd.CALC_DT
  INNER JOIN (select * from ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_lost_sales_value where UPPER(RECORD_SOURCE_CD) = 'LOESS') AS llsv
    ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = llsv.SALES_DT
  INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_forecast_baseline_unit lfbu
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

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_osa_phantom_inventory AS
WITH CALC_DATE AS
( SELECT
     ALERT_DT
     , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
   FROM (SELECT
           SALES_DT AS ALERT_DT
           , SALES_DT AS START_SALES_DT
           , DATE_ADD(CAST(SALES_DT AS DATE), -4) AS END_SALES_DT
         FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_drfe_alerts_osa_sales_dt) TMP
   LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
, SALES_DATA AS
( SELECT
    sles.HUB_ORGANIZATION_UNIT_HK
    , sles.HUB_RETAILER_ITEM_HK
    , msd.ALERT_DT
    , sles.SALES_DT
    , llsv.LOST_SALES_AMT
  FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
  INNER JOIN CALC_DATE msd
    ON sles.SALES_DT = msd.CALC_DT
  INNER JOIN (select * from ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_lost_sales_value where UPPER(RECORD_SOURCE_CD) = 'DRFE') AS llsv
    ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = llsv.SALES_DT
  INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_forecast_baseline_unit lfbu
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

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_osa_availability_voids AS
WITH CALC_DATE AS
( SELECT
    ALERT_DT
    , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
  FROM (SELECT
          SALES_DT AS ALERT_DT
          , SALES_DT AS START_SALES_DT
          , DATE_ADD(CAST(SALES_DT AS DATE), -1) AS END_SALES_DT
        FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_loess_alerts_osa_sales_dt) TMP
  LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
, SALES_DATA AS
( SELECT
    sles.HUB_ORGANIZATION_UNIT_HK
    , sles.HUB_RETAILER_ITEM_HK
    , msd.ALERT_DT
    , sles.SALES_DT
    , llsv.LOST_SALES_AMT
  FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
  INNER JOIN  CALC_DATE msd
    ON sles.SALES_DT = msd.CALC_DT
  INNER JOIN (select * from ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_lost_sales_value where UPPER(RECORD_SOURCE_CD) = 'LOESS') llsv
    ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = llsv.SALES_DT
  INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_forecast_baseline_unit lfbu
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

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_osa_availability_voids AS
WITH CALC_DATE AS
( SELECT
    ALERT_DT
    , DATE_ADD(TMP.END_SALES_DT, PE.i) AS CALC_DT
  FROM (SELECT
          SALES_DT AS ALERT_DT
          , SALES_DT AS START_SALES_DT
          , DATE_ADD(CAST(SALES_DT AS DATE), -1) AS END_SALES_DT
        FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_drfe_alerts_osa_sales_dt) TMP
  LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.START_SALES_DT, TMP.END_SALES_DT)), ' ')) PE AS i , X)
, SALES_DATA AS
( SELECT
    sles.HUB_ORGANIZATION_UNIT_HK
    , sles.HUB_RETAILER_ITEM_HK
    , msd.ALERT_DT
    , sles.SALES_DT
    , llsv.LOST_SALES_AMT
  FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
  INNER JOIN  CALC_DATE msd
    ON sles.SALES_DT = msd.CALC_DT
  INNER JOIN (select * from ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_lost_sales_value where UPPER(RECORD_SOURCE_CD) = 'DRFE') llsv
    ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT = llsv.SALES_DT
  INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_forecast_baseline_unit lfbu
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

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_osa_slow_sales AS
SELECT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , "Slow Sales" AS ALERT_MESSAGE_DESC
  , 3 AS ALERT_PRIORITY_ID
  , adsd.SALES_DT
  , SUM(llsv.LOST_SALES_AMT) AS LOST_SALES_AMT
FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_loess_alerts_osa_sales_dt adsd
  ON sles.SALES_DT <= adsd.SALES_DT
  AND sles.SALES_DT >= DATE_ADD(CAST(adsd.SALES_DT AS DATE), -1)
INNER JOIN (select * from ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_lost_sales_value where UPPER(RECORD_SOURCE_CD) = 'LOESS') llsv
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

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_osa_slow_sales AS
SELECT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , "Slow Sales" AS ALERT_MESSAGE_DESC
  , 3 AS ALERT_PRIORITY_ID
  , adsd.SALES_DT
  , SUM(llsv.LOST_SALES_AMT) AS LOST_SALES_AMT
FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_drfe_alerts_osa_sales_dt adsd
  ON sles.SALES_DT <= adsd.SALES_DT
  AND sles.SALES_DT >= DATE_ADD(CAST(adsd.SALES_DT AS DATE), -1)
INNER JOIN (select * from ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_lost_sales_value where UPPER(RECORD_SOURCE_CD) = 'DRFE') llsv
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

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_inventory_cleanup_store_product AS
SELECT DISTINCT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , sles.SALES_DT
  , sles.ON_HAND_INVENTORY_QTY
  , slesup.UNIT_PRICE_AMT
FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_loess_alerts_invcleanup_sales_dt adsd
  ON sles.SALES_DT = adsd.SALES_DT
INNER JOIN ${Retailer_Client}_${Country_Code}_dv.vw_sat_retailer_item_unit_price slesup
  ON sles.HUB_ORGANIZATION_UNIT_HK = slesup.HUB_ORGANIZATION_UNIT_HK
  AND sles.HUB_RETAILER_ITEM_HK = slesup.HUB_RETAILER_ITEM_HK
     AND sles.SALES_DT = slesup.SALES_DT
WHERE
  sles.ON_HAND_INVENTORY_QTY > 0;  

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_inventory_cleanup_store_product AS
SELECT DISTINCT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , sles.SALES_DT
  , sles.ON_HAND_INVENTORY_QTY
  , slesup.UNIT_PRICE_AMT
FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.tmp_hold_audit_drfe_alerts_invcleanup_sales_dt adsd
  ON sles.SALES_DT = adsd.SALES_DT
INNER JOIN ${Retailer_Client}_${Country_Code}_dv.vw_sat_retailer_item_unit_price slesup
  ON sles.HUB_ORGANIZATION_UNIT_HK = slesup.HUB_ORGANIZATION_UNIT_HK
  AND sles.HUB_RETAILER_ITEM_HK = slesup.HUB_RETAILER_ITEM_HK
     AND sles.SALES_DT = slesup.SALES_DT
WHERE
  sles.ON_HAND_INVENTORY_QTY > 0;  

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_inventory_cleanup_store_product_weekly_avg_units AS
SELECT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , icsp.SALES_DT
  , AVG(CASE WHEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0) > nvl(sles.POS_ITEM_QTY, 0)
          THEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0)
          ELSE nvl(sles.POS_ITEM_QTY, 0)
        END) * 7 AS WEEKLY_AVG_POS_ITEM_QTY
FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_inventory_cleanup_store_product icsp
    ON sles.HUB_ORGANIZATION_UNIT_HK = icsp.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = icsp.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT <= icsp.SALES_DT
    AND sles.SALES_DT >= DATE_ADD(icsp.SALES_DT, -21)
LEFT JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_loess_forecast_baseline_unit lfbu
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

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_inventory_cleanup_store_product_weekly_avg_units AS
SELECT
  sles.HUB_ORGANIZATION_UNIT_HK
  , sles.HUB_RETAILER_ITEM_HK
  , icsp.SALES_DT
  , AVG(CASE WHEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0) > nvl(sles.POS_ITEM_QTY, 0)
          THEN nvl(lfbu.BASELINE_POS_ITEM_QTY, 0)
          ELSE nvl(sles.POS_ITEM_QTY, 0)
        END) * 7 AS WEEKLY_AVG_POS_ITEM_QTY
FROM ${Retailer_Client}_${Country_Code}_dv.vw_sat_link_epos_summary sles
INNER JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_inventory_cleanup_store_product icsp
    ON sles.HUB_ORGANIZATION_UNIT_HK = icsp.HUB_ORGANIZATION_UNIT_HK
    AND sles.HUB_RETAILER_ITEM_HK = icsp.HUB_RETAILER_ITEM_HK
    AND sles.SALES_DT <= icsp.SALES_DT
    AND sles.SALES_DT >= DATE_ADD(icsp.SALES_DT, -21)
LEFT JOIN ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_drfe_forecast_baseline_unit lfbu
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

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_alert_inventory_cleanup
AS
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , HUB_RETAILER_ITEM_HK
  , ALERT_MESSAGE_DESC
  , ALERT_TYPE_NM
  , LOST_SALES_AMT
  , WEEKS_COVER_NUM
  , ON_HAND_INVENTORY_QTY
  , SALES_DT
  , RECORD_SOURCE_CD
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , ALERT_MESSAGE_DESC
        , ALERT_TYPE_NM
        , LOST_SALES_AMT
        , WEEKS_COVER_NUM
        , ON_HAND_INVENTORY_QTY
        , SALES_DT
        , RECORD_SOURCE_CD
        , ROW_NUMBER() OVER (PARTITION BY SALES_DT, RECORD_SOURCE_CD, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK ORDER BY LOAD_TS DESC) rnum
      FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.alert_inventory_cleanup) z
WHERE
  rnum = 1;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_alert_on_shelf_availability
AS
SELECT
  HUB_ORGANIZATION_UNIT_HK
  , HUB_RETAILER_ITEM_HK
  , ALERT_MESSAGE_DESC
  , ALERT_TYPE_NM
  , ON_HAND_INVENTORY_QTY
  , LOST_SALES_AMT
  , SALES_DT
  , RECORD_SOURCE_CD
FROM (SELECT
        HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , ALERT_MESSAGE_DESC
        , ALERT_TYPE_NM
        , ON_HAND_INVENTORY_QTY
        , LOST_SALES_AMT
        , SALES_DT
        , RECORD_SOURCE_CD
        , ROW_NUMBER() OVER (PARTITION BY SALES_DT, RECORD_SOURCE_CD, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK ORDER BY LOAD_TS DESC) rnum
      FROM ${Retailer_Client}_${Country_Code}_retail_alert_im.alert_on_shelf_availability) z
WHERE
  rnum = 1;

-- COMMAND ----------

create or replace view ${Retailer_Client}_${Country_Code}_retail_alert_im.vw_fct_agg_ty_ly_sales as 
select * from (
select coalesce(ty.week, ly.week) as week,
coalesce(ty.store_nbr, ly.store_nbr) as store_nbr,
coalesce(ty.item_nbr, ly.item_nbr) as item_nbr,
ty.pos_sales as ty_pos_sales,
ty.pos_qty as ty_pos_qty,
ty.on_hand_inventory_qty as ty_on_hand_inventory_qty,
ly.pos_sales as ly_pos_sales,
ly.pos_qty as ly_pos_qty,
ly.on_hand_inventory_qty as ly_on_hand_inventory_qty
from (
SELECT 
cast(replace(cast(gcsl.week as string),'-','') as int) as week,
hstore.organization_unit_num as store_nbr,
hitem.retailer_item_id as item_nbr,
sum(epos.pos_amt) as pos_sales,
sum(epos.pos_item_qty) as pos_qty,
max(case when date_format(epos.sales_dt, 'EEEE') == "Friday" then epos.on_hand_inventory_qty else null end) as on_hand_inventory_qty
FROM 
${Retailer_Client}_${Country_Code}_dv.VW_SAT_LINK_EPOS_SUMMARY epos
inner join enterprise_dv.gregorian_calendar gc on epos.sales_dt = gc.day_date
inner join enterprise_dv.vw_gregorian_calendar_52wk_sliding gcsl on gc.CLNDR_WK_STRT_DATE = gcsl.week
inner join ${Retailer_Client}_${Country_Code}_dv.hub_retailer_item hitem on hitem.hub_retailer_item_hk = epos.hub_retailer_item_hk
inner join ${Retailer_Client}_${Country_Code}_dv.hub_organization_unit hstore on hstore.hub_organization_unit_hk = epos.hub_organization_unit_hk
group by gcsl.week, hstore.organization_unit_num, hitem.retailer_item_id ) ty
full outer join (
SELECT 
cast(replace(cast(gcsl.week as string),'-','') as int) as week,
hstore.organization_unit_num as store_nbr,
hitem.retailer_item_id as item_nbr,
sum(epos.pos_amt) as pos_sales,
sum(epos.pos_item_qty) as pos_qty,
max(case when date_format(epos.sales_dt, 'EEEE') == "Friday" then epos.on_hand_inventory_qty else null end) as on_hand_inventory_qty
FROM 
${Retailer_Client}_${Country_Code}_dv.VW_SAT_LINK_EPOS_SUMMARY epos
inner join enterprise_dv.gregorian_calendar gc on epos.sales_dt = gc.day_date
inner join enterprise_dv.vw_gregorian_calendar_52wk_sliding gcsl on gc.CLNDR_WK_STRT_DATE = gcsl.week_ly
inner join ${Retailer_Client}_${Country_Code}_dv.hub_retailer_item hitem on hitem.hub_retailer_item_hk = epos.hub_retailer_item_hk
inner join ${Retailer_Client}_${Country_Code}_dv.hub_organization_unit hstore on hstore.hub_organization_unit_hk = epos.hub_organization_unit_hk
group by gcsl.week, hstore.organization_unit_num, hitem.retailer_item_id ) ly on ty.week = ly.week and ty.store_nbr = ly.store_nbr and ty.item_nbr = ly.item_nbr )

