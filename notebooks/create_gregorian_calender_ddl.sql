-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "azueus2devadlsdatalake", "ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val ADLS_Account_Name = dbutils.widgets.get("ADLS_Account_Name")

-- COMMAND ----------

drop table if exists enterprise_dv.gregorian_calendar;

-- COMMAND ----------


CREATE TABLE  enterprise_dv.gregorian_calendar
(DAY_ID int,
DAY_DATE date,
DAY_WKDAY_NM string,
DAY_WKDAY_SHNM string,
DAY_CLNDR_DAY_OF_YR_NUM int,
CLNDR_MTH_NUM int,
CLNDR_MTH_NM string,
CLNDR_MTH_SHNM string,
CLNDR_QTR_NUM int,
CLNDR_QTR_NM string,
CLNDR_YR_NUM int,
CLNDR_MTH_STRT_DATE date,
CLNDR_MTH_END_DATE date,
CLNDR_WK_NUM int,
CLNDR_WK_STRT_DATE date,
CLNDR_WK_END_DATE date)
 USING com.databricks.spark.csv
OPTIONS (
 multiLine 'false',
 serialization.format  '1',
 header 'true',
 delimiter ',',
 encoding 'UTF-8',
 dateFormat = "MM/dd/yyyy",
 timestampFormat = "MM/dd/yyyy HH:mm:ss",
 ignoreTrailingWhiteSpace 'true',
 ignoreLeadingWhiteSpace 'true',
 path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/edw/gregorian_calendar/gregorian_calendar_static.csv'
);
 

   


-- COMMAND ----------

CREATE VIEW `enterprise_dv`.`vw_gregorian_calendar`(DAY_ID, DAY_DATE, DAY_WKDAY_NM, DAY_WKDAY_SHNM, DAY_CLNDR_DAY_OF_YR_NUM, CLNDR_MTH_NUM, CLNDR_MTH_NM, CLNDR_MTH_SHNM, CLNDR_QTR_NUM, CLNDR_QTR_NM, CLNDR_YR_NUM, CLNDR_MTH_STRT_DATE, CLNDR_MTH_END_DATE, CLNDR_WK_NUM, CLNDR_WK_STRT_DATE, CLNDR_WK_END_DATE, week_id, week_id_prev, CLNDR_WK_STRT_DATE_LY) AS
select distinct gc.*,gcp.CLNDR_WK_STRT_DATE as CLNDR_WK_STRT_DATE_LY from (
(select *, week_id-52 as week_id_prev from (
select *, dense_rank() over (order by CLNDR_WK_STRT_DATE asc ) as week_id  from enterprise_dv.gregorian_calendar ) ) gc
inner join (select *, dense_rank() over (order by CLNDR_WK_STRT_DATE asc ) as week_id  from enterprise_dv.gregorian_calendar) gcp on gc.week_id_prev = gcp.week_id
)


-- COMMAND ----------

create or replace view enterprise_dv.vw_gregorian_calendar_52wk_sliding as 

select dense_rank() over (order by x.week) as week_seq, week, week_ly from ( 
select distinct
wc.CLNDR_WK_STRT_DATE  as week,  
wcly.CLNDR_WK_STRT_DATE  as week_ly
from  enterprise_dv.vw_gregorian_calendar wc
inner join enterprise_dv.vw_gregorian_calendar wcly on wc.CLNDR_WK_STRT_DATE_LY = wcly.CLNDR_WK_STRT_DATE 
where wc.day_date between date_add(current_Date(),-364) and date_add(current_Date(),-7) ) x
order by week_seq
