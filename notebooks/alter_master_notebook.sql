-- Databricks notebook source
-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Retailer_Client")
-- MAGIC dbutils.widgets.get("ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mv("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/audit_alerts_osa_last_processed_ts",
-- MAGIC              "adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/audit_loess_alerts_osa_last_processed_ts", true)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mv("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/audit_alerts_invcleanup_last_processed_ts",
-- MAGIC              "adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/audit_loess_alerts_invcleanup_last_processed_ts", true)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/lost_sales_value")

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists lost_sales_value;
CREATE TABLE `lost_sales_value` (`HUB_ORGANIZATION_UNIT_HK` STRING, `HUB_RETAILER_ITEM_HK` STRING, `LOAD_TS` TIMESTAMP, `LOST_SALES_AMT` DECIMAL(15,2), `SALES_DT` DATE, `RECORD_SOURCE_CD` STRING)
USING org.apache.spark.sql.parquet
OPTIONS (
  `compression` 'snappy',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/lost_sales_value'
)
PARTITIONED BY (SALES_DT, RECORD_SOURCE_CD);

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
SET spark.sql.shuffle.partitions=10;
insert into lost_sales_value partition (SALES_DT,RECORD_SOURCE_CD)
select HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, LOAD_TS, LOST_SALES_AMT, SALES_DT, 'loess' from loess_lost_sales_value;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
select 'loess_lost_sales_value', count(*) TotalRows from loess_lost_sales_value
union
select 'lost_sales_value', count(*) TotalRows from lost_sales_value;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
show create table loess_lost_sales_value;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists loess_lost_sales_value;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists alert_on_shelf_availability_backup;
CREATE TABLE `alert_on_shelf_availability_backup` (`HUB_ORGANIZATION_UNIT_HK` STRING, `HUB_RETAILER_ITEM_HK` STRING, `LOAD_TS` TIMESTAMP, `RECORD_SOURCE_CD` STRING, `ALERT_MESSAGE_DESC` STRING, `ALERT_TYPE_NM` STRING, `ON_HAND_INVENTORY_QTY` DECIMAL(15,2), `LOST_SALES_AMT` DECIMAL(15,2), `SALES_DT` DATE)
USING org.apache.spark.sql.parquet
OPTIONS (
  `compression` 'snappy',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/alert_on_shelf_availability_backup'
)
PARTITIONED BY (SALES_DT);
msck repair table alert_on_shelf_availability_backup;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/alert_on_shelf_availability")

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists alert_on_shelf_availability;
CREATE TABLE `alert_on_shelf_availability` (`HUB_ORGANIZATION_UNIT_HK` STRING, `HUB_RETAILER_ITEM_HK` STRING, `LOAD_TS` TIMESTAMP, `ALERT_MESSAGE_DESC` STRING, `ALERT_TYPE_NM` STRING, `ON_HAND_INVENTORY_QTY` DECIMAL(15,2), `LOST_SALES_AMT` DECIMAL(15,2), `SALES_DT` DATE, `RECORD_SOURCE_CD` STRING)
USING org.apache.spark.sql.parquet
OPTIONS (
  `compression` 'snappy',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/alert_on_shelf_availability'
)
PARTITIONED BY (SALES_DT,RECORD_SOURCE_CD);

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
SET spark.sql.shuffle.partitions=10;
insert into alert_on_shelf_availability partition (SALES_DT,RECORD_SOURCE_CD)
select HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, LOAD_TS, ALERT_MESSAGE_DESC, ALERT_TYPE_NM, ON_HAND_INVENTORY_QTY, LOST_SALES_AMT, SALES_DT, 'loess' from alert_on_shelf_availability_backup;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
select 'alert_on_shelf_availability_backup', count(*) TotalRows from alert_on_shelf_availability_backup
union
select 'alert_on_shelf_availability', count(*) TotalRows from alert_on_shelf_availability_backup;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
show create table alert_on_shelf_availability_backup;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists alert_on_shelf_availability_backup;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists alert_inventory_cleanup_backup;
CREATE TABLE `alert_inventory_cleanup_backup` (`HUB_ORGANIZATION_UNIT_HK` STRING, `HUB_RETAILER_ITEM_HK` STRING, `LOAD_TS` TIMESTAMP, `RECORD_SOURCE_CD` STRING, `ALERT_MESSAGE_DESC` STRING, `ALERT_TYPE_NM` STRING, `ON_HAND_INVENTORY_QTY` DECIMAL(15,2), `LOST_SALES_AMT` DECIMAL(15,2), `WEEKS_COVER_NUM` DECIMAL(15,2), `SALES_DT` DATE)
USING org.apache.spark.sql.parquet
OPTIONS (
  `compression` 'snappy',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/alert_inventory_cleanup_backup'
)
PARTITIONED BY (SALES_DT);
msck repair table alert_inventory_cleanup_backup;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/alert_inventory_cleanup")

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists alert_inventory_cleanup;
CREATE TABLE `alert_inventory_cleanup` (`HUB_ORGANIZATION_UNIT_HK` STRING, `HUB_RETAILER_ITEM_HK` STRING, `LOAD_TS` TIMESTAMP, `ALERT_MESSAGE_DESC` STRING, `ALERT_TYPE_NM` STRING, `ON_HAND_INVENTORY_QTY` DECIMAL(15,2), `LOST_SALES_AMT` DECIMAL(15,2), `WEEKS_COVER_NUM` DECIMAL(15,2), `SALES_DT` DATE, `RECORD_SOURCE_CD` STRING)
USING org.apache.spark.sql.parquet
OPTIONS (
  `compression` 'snappy',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/alert_inventory_cleanup'
)
PARTITIONED BY (SALES_DT,RECORD_SOURCE_CD);

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
SET spark.sql.shuffle.partitions=10;
insert into alert_inventory_cleanup partition (SALES_DT,RECORD_SOURCE_CD)
select HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, LOAD_TS, ALERT_MESSAGE_DESC, ALERT_TYPE_NM, ON_HAND_INVENTORY_QTY, LOST_SALES_AMT, WEEKS_COVER_NUM, SALES_DT, 'loess' from alert_inventory_cleanup_backup;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
select 'alert_inventory_cleanup_backup', count(*) TotalRows from alert_inventory_cleanup_backup
union
select 'alert_inventory_cleanup', count(*) TotalRows from alert_inventory_cleanup;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
show create table alert_inventory_cleanup_backup;

-- COMMAND ----------

use ${Retailer_Client}_retail_alert_im;
drop table if exists alert_inventory_cleanup_backup;

