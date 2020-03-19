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

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create ADLS_Account_Name Widget

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Read Retailer_Client and ADLS_Account_Name widget values in Scala local variables
-- MAGIC 
-- MAGIC These values are used by various Scala code

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val retailerClient = dbutils.widgets.get("Retailer_Client")
-- MAGIC val adlsAccountNm = dbutils.widgets.get("ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Refreshing the table so that files loaded from Data Vault processes are registered into Metastore

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_dv.AUDIT_DRIVER_SALES_DATES;

-- COMMAND ----------

--INSERT OVERWRITE TABLE walmart_barilla_retail_alert_im.AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS VALUES('2018-07-01 00:00:00', '2018-07-01 00:00:00')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit the notebook if there is no last processed timestamp present
-- MAGIC 
-- MAGIC This would potentially happen in the very first run where the process expects some value stored in table AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS to let the process know which dates to process.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${retailerClient}_retail_alert_im.AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS")
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
-- MAGIC We want to hold them at the beginning of the process so that when we update AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS at the end of the process, we do not have to refer back to AUDIT_DRIVER_SALES_DATES which may have new sales dates loaded.

-- COMMAND ----------

DROP TABLE IF EXISTS ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS;
CREATE TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS AS
SELECT
  A.SALES_DT,
  date_format(A.LOAD_TS, "yyyy-MM-dd hh:mm:ss") AS LOAD_TS
FROM
  ${Retailer_Client}_dv.AUDIT_DRIVER_SALES_DATES A,
  ${Retailer_Client}_retail_alert_im.AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS B
WHERE
A.LOAD_TS > B.LOAD_TS;
REFRESH TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit the notebook if there is no new dates to process

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.sql(f"SELECT * FROM ${retailerClient}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS")
-- MAGIC val count = df.count
-- MAGIC println(count)
-- MAGIC if (count == 0 ){
-- MAGIC   // Drop the temp table TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS before exiting the notebook
-- MAGIC   spark.sql(f"DROP TABLE ${retailerClient}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS")
-- MAGIC   dbutils.notebook.exit("0")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Get +7 days for all the dates identified above, get the unique list of store-product for all sales dates, CROSS Join them with +7 dates and load to a TEMP table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### Delete the table directory on ADLS for the temp table TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/informationmart/" + dbutils.widgets.get("Retailer_Client") + "_retail_alert/tmp_loess_forecast_baseline_unit_future_sales_data", true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Running this query just to show which future dates are added

-- COMMAND ----------

SELECT DISTINCT
    DATE_ADD(TMP.START_SALES_DATE,PE.i) AS FUTURE_SALES_DT
FROM
      (
        SELECT DATE_ADD(SALES_DT, 1) AS START_SALES_DATE,
               DATE_ADD(SALES_DT, 7) AS END_SALES_DATE
        FROM
             ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS B
      ) TMP 
  LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.END_SALES_DATE,TMP.START_SALES_DATE)),' ')) PE AS i, X
ORDER BY 1;

-- COMMAND ----------

SET spark.sql.shuffle.partitions=10;
--SET spark.sql.autoBroadcastJoinThreshold=-1;

-- Get +7 dates for the sales data loaded to the Data Vault in the current batch
-- These are the potential dates we will forecast
REFRESH TABLE ${Retailer_Client}_retail_alert_im.LOESS_FORECAST_BASELINE_UNIT;
--#########################################################################################
-- Drop the temp table if exists
--#########################################################################################
DROP TABLE IF EXISTS ${Retailer_Client}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA;

--#########################################################################################
-- Create the temp table with select statement and load data
--#########################################################################################

CREATE TABLE ${Retailer_Client}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_retail_alert/tmp_loess_forecast_baseline_unit_future_sales_data')


-- Get Date + 1 to Date + 7 for all the sales dates loaded in Data Vault in the current batch
WITH FUTURE_SALES_DATE AS
(
  SELECT DISTINCT
      DATE_ADD(TMP.START_SALES_DATE,PE.i) AS FUTURE_SALES_DT
  FROM
        (
          SELECT DATE_ADD(SALES_DT, 1) AS START_SALES_DATE,
                 DATE_ADD(SALES_DT, 7) AS END_SALES_DATE
          FROM
               ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS B

        ) TMP 
    LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(TMP.END_SALES_DATE,TMP.START_SALES_DATE)),' ')) PE AS i, X
),

-- Get list of store and product hash keys for the sales data loaded to the Data Vault in the current batch
STORE_PRODUCT_LIST
(
  SELECT DISTINCT
      HUB_Organization_Unit_HK,
      HUB_Retailer_Item_HK
  FROM
    ${Retailer_Client}_dv.LINK_ePOS_Summary LINK,
    ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS CONF

  WHERE
    CONF.SALES_DT = LINK.SALES_DT
)

-- Cross join the future dates and all store products combinations from the sales dates loaded to Data vault in the current batch

SELECT 

  SPL.HUB_Organization_Unit_HK AS HUB_Organization_Unit_HK,
  SPL.HUB_Retailer_Item_HK AS HUB_Retailer_Item_HK,
  FDT.FUTURE_SALES_DT AS SALES_DT

FROM
  STORE_PRODUCT_LIST AS SPL CROSS JOIN FUTURE_SALES_DATE FDT

-- Exclude the records already loaded to the target forecasated table
-- This condition will bring the missing store-products for already forecasted dates and
-- load the new ones for the dates that have not been forecasted yet [ Need to check with Veg to make sure this makes sense ]
WHERE
  NOT EXISTS
  (
    SELECT 1 FROM ${Retailer_Client}_retail_alert_im.LOESS_FORECAST_BASELINE_UNIT F 
    WHERE 
      F.SALES_DT = FDT.FUTURE_SALES_DT AND
      F.HUB_Organization_Unit_HK = SPL.HUB_Organization_Unit_HK AND
      F.HUB_Retailer_Item_HK = SPL.HUB_Retailer_Item_HK
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### Collect statistics for the temp table

-- COMMAND ----------

ANALYZE TABLE ${Retailer_Client}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA COMPUTE STATISTICS FOR COLUMNS HUB_Organization_Unit_HK, HUB_Retailer_Item_HK;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### This method gets the list of past 12 week same day dates for all the sales dates loaded to Data Vault in the current batch and returns a List of those dates as string in YYYY-MM-DD format

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##Method: getPastNWeekSameDay
-- MAGIC 
-- MAGIC This method takes a date as a string in YYYY-MM-DD format and number of weeks to go in past. It returns a list of dates which are same day dates respect to given date from past N weeks
-- MAGIC 
-- MAGIC **Example:**
-- MAGIC 
-- MAGIC **getPastNWeekSameDay("2018-07-04", 12)** returns:
-- MAGIC 
-- MAGIC '2018-04-11', '2018-04-18', '2018-04-25', '2018-05-02', '2018-05-09', '2018-05-16', '2018-05-23', '2018-05-30', '2018-06-06', '2018-06-13', '2018-06-20', '2018-06-27'

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import java.text.SimpleDateFormat
-- MAGIC import java.util.Calendar
-- MAGIC import scala.collection.mutable.ListBuffer
-- MAGIC 
-- MAGIC def getPastNWeekSameDay(date: String, weeks: Int) : List[String] = {
-- MAGIC 
-- MAGIC   val dateAux = Calendar.getInstance()
-- MAGIC   val range = Array.range(1, weeks + 1).toList
-- MAGIC   val outputList = new ListBuffer[String]()
-- MAGIC   
-- MAGIC   range.foreach(wk => {
-- MAGIC      dateAux.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(date))
-- MAGIC     dateAux.add(Calendar.DATE, wk * -7)
-- MAGIC     outputList += "'" + new SimpleDateFormat("yyyy-MM-dd").format(dateAux.getTime()) + "'"
-- MAGIC   })
-- MAGIC  outputList.toList.sorted
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Start collecting past 12 week same days  - This runs in LOOP in the current SQL Server design

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Method: TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA
-- MAGIC This method reads a date as an argument and runs a SQL query to load table TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Step 1:** Create a string variable to hold the query<br>
-- MAGIC **Step 2:** Drop the temp table if exists<br>
-- MAGIC **Step 3:** Remove the table directory from ADLS as this is an external table and dropping table does not delete the actual data<br>
-- MAGIC **Step 4:** Execute the sql which loads data into the temp table<br>
-- MAGIC **Step 5:** Collect statistics on the temp table
-- MAGIC 
-- MAGIC This list will drive the loop for the next steps

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC def Load_TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA(forecastDate: String): Unit = {
-- MAGIC // Define query to load table TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA
-- MAGIC //SET spark.sql.shuffle.partitions=10;  
-- MAGIC   
-- MAGIC   spark.conf.set("spark.sql.shuffle.partitions","10")
-- MAGIC   spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
-- MAGIC   
-- MAGIC   //Get past N same day dates list
-- MAGIC   val pastDateList = getPastNWeekSameDay(forecastDate, 12)
-- MAGIC   
-- MAGIC   val sql = f"""
-- MAGIC               CREATE TABLE ${retailerClient}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA
-- MAGIC               USING org.apache.spark.sql.parquet
-- MAGIC               PARTITIONED BY (HISTORY_SALES_DT)
-- MAGIC               OPTIONS ('compression' 'snappy', path 'adl://${adlsAccountNm}.azuredatalakestore.net/informationmart/${retailerClient}_retail_alert/tmp_loess_forecast_baseline_unit_history_sales_data')
-- MAGIC 
-- MAGIC               WITH TMP_PAST12_WEEKS_SAME_DAY_DATA AS
-- MAGIC               (
-- MAGIC               SELECT
-- MAGIC                        HUB_Organization_Unit_HK,
-- MAGIC                        HUB_Retailer_Item_HK,
-- MAGIC                        POS_Item_QTY,
-- MAGIC                        POS_AMT,
-- MAGIC                        On_Hand_Inventory_QTY,
-- MAGIC                        SALES_DT
-- MAGIC                   FROM
-- MAGIC                      ${retailerClient}_dv.VW_SAT_LINK_EPOS_SUMMARY
-- MAGIC 
-- MAGIC                   WHERE
-- MAGIC                         SALES_DT IN (${pastDateList.mkString(",")})
-- MAGIC               )
-- MAGIC 
-- MAGIC                 SELECT DISTINCT
-- MAGIC                      CUR.SALES_DT AS CURRENT_SALES_DT,
-- MAGIC                      HIST.POS_Item_QTY AS HIST_POS_Item_QTY,
-- MAGIC                      CUR.HUB_Organization_Unit_HK,
-- MAGIC                      CUR.HUB_Retailer_Item_HK,
-- MAGIC                      HIST.SALES_DT AS HISTORY_SALES_DT
-- MAGIC                 FROM
-- MAGIC                   ${retailerClient}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA CUR,
-- MAGIC                   TMP_PAST12_WEEKS_SAME_DAY_DATA HIST
-- MAGIC                 WHERE
-- MAGIC                   CUR.SALES_DT = '${forecastDate}' AND
-- MAGIC                   CUR.HUB_Organization_Unit_HK = HIST.HUB_Organization_Unit_HK AND
-- MAGIC                   CUR.HUB_Retailer_Item_HK = HIST.HUB_Retailer_Item_HK"""
-- MAGIC 
-- MAGIC 
-- MAGIC   // Drop the temp table if exists
-- MAGIC   spark.sql(f"DROP TABLE IF EXISTS ${retailerClient}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA")
-- MAGIC 
-- MAGIC   // Delete the table directory from ADLS
-- MAGIC   dbutils.fs.rm(f"adl://${adlsAccountNm}.azuredatalakestore.net/informationmart/${retailerClient}_retail_alert/tmp_loess_forecast_baseline_unit_history_sales_data", true)
-- MAGIC 
-- MAGIC   // Run the SQL statement
-- MAGIC   spark.sql(sql)
-- MAGIC   
-- MAGIC   // Collect statistics on the temp table
-- MAGIC   spark.sql(f"ANALYZE TABLE ${retailerClient}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA COMPUTE STATISTICS FOR COLUMNS HUB_Organization_Unit_HK, HUB_Retailer_Item_HK")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Method: GetFutureDates
-- MAGIC 
-- MAGIC This method reads unique SALES_DT from temp table TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA and returns these dates as a list of string.
-- MAGIC These are the dates need to be forecasted.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC def GetFutureDates(): List[String] = {
-- MAGIC 
-- MAGIC   // Define SQL statement to get future dates
-- MAGIC   val sqlFutureDays = f"""SELECT DISTINCT CAST(SALES_DT AS STRING) AS SALES_DT FROM ${retailerClient}_retail_alert_im.TMP_LOESS_FORECAST_BASELINE_UNIT_FUTURE_SALES_DATA """
-- MAGIC   
-- MAGIC   // Run the SQL statement
-- MAGIC   val result = spark.sql(sqlFutureDays)
-- MAGIC   
-- MAGIC   // Return the dates from query result as a List of Strings
-- MAGIC   result.select("SALES_DT").map(r => r.getString(0)).collect.toList.sorted
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### View: VW_LOESS_FORECAST_BASELINE_UNIT_TRICUBE_WEIGHT
-- MAGIC Derive TriCube Weight function as a view for further calculation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Step 1: Derive distance as a rank on store-product ordered by sales date in desc order. Closer dates gets smaller distance compared to farther dates.<br>
-- MAGIC Step 2: Calculate Tricube weight as  ( 1 - ( distance / max distance ) ^ 3 ) ^3. This weight is calculated for only those store-products that have sales for atleast 2 weeks in past otherwise the set to NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC 
-- MAGIC #### View: VW_GET_LOESS_FORECAST_BASELINE_UNIT
-- MAGIC Calculate LOESS baseline values as a view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC This step follows below logic to implement LOESS algorithm.<br>
-- MAGIC 
-- MAGIC y = mx + c<br>
-- MAGIC Here y is the dependent variable x is the independent variable. m is the slope of the line and c is the intercept on Y-axis<br>
-- MAGIC y = (slope)x + (Y-axis intercept)<br>
-- MAGIC 
-- MAGIC 
-- MAGIC m = n∑xy  -  ∑x∑y / n∑x²  - (∑x)²<br>
-- MAGIC c = ∑x²∑y  -  ∑x∑xy / / n∑x²  - (∑x)²<br>
-- MAGIC  
-- MAGIC SumWts       = SUM(W) Group By ChainRefExternal, RefExternal<br>
-- MAGIC SumWtX       = SUM(X * W ) Group By ChainRefExternal, RefExternal ===============> ∑x<br>
-- MAGIC SumWtX2      = SUM(X² * W) Group By ChainRefExternal, RefExternal ===============> ∑x²<br>
-- MAGIC SumWtY       = SUM(Y * W) Group By ChainRefExternal, RefExternal ================> ∑y<br>
-- MAGIC SumWtXY      = SUM(X * Y * W) Group By ChainRefExternal, RefExternal ============> ∑xy<br>
-- MAGIC Denom        = (SumWts * SumWtX2) - SumWtX²<br>
-- MAGIC              = ( SUM(W) * SUM(X² * W) ) - SUM(X * W )² ) ========================> n∑x²  - (∑x)²<br>
-- MAGIC WLRSlope     = (SumWts * SumWtXY - SumWtX * SumWtY) / Denom =====================> m<br>
-- MAGIC WLRIntercept = (SumWtX2 * SumWtY - SumWtX * SumWtXY) / Denom ====================> c<br>
-- MAGIC 
-- MAGIC 
-- MAGIC **Step 1:** Derive colums SUM_TRICUBE_WEIGHT, SUM_TRICUBE_WEIGHT_X_Axis, SUM_TRICUBE_WEIGHT_X_Axis_Squared, SUM_TRICUBE_WEIGHT_Y_Axis, SUM_TRICUBE_WEIGHT_X_Axis_Y_Axis. These columns are used in the following calculations.<br>
-- MAGIC 
-- MAGIC **Here the past 12 weeks are on X-Axis as 12 data points. Time flows from left ro right so first point is the oldest week and the last point would be most recent week**<br>
-- MAGIC **Step 2:** Derive slope and Y-Axis intercept as per the equations above<br>
-- MAGIC **Step 3:** Derive baseline units as mx + c as per the equations
-- MAGIC 
-- MAGIC 
-- MAGIC **Note: ** These columns are calculated for only those store-products that have sales for atleast 2 weeks in past otherwise the set to NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Method: LoessForecast
-- MAGIC Loads the target table from the view VW_GET_LOESS_FORECASTED

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC def LoessForecast(): Unit = {
-- MAGIC val sql = f"""
-- MAGIC INSERT INTO ${retailerClient}_retail_alert_im.LOESS_FORECAST_BASELINE_UNIT PARTITION(SALES_DT)
-- MAGIC SELECT
-- MAGIC   HUB_ORGANIZATION_UNIT_HK,
-- MAGIC   HUB_RETAILER_ITEM_HK,  
-- MAGIC   CURRENT_TIMESTAMP AS LOAD_TS,
-- MAGIC   'db.nb.load_loess_forecast_baseline_unit' AS RECORD_SOURCE_CD,
-- MAGIC   Y_AXIS_FORECASTED AS BASELINE_POS_ITEM_QTY,
-- MAGIC   SALES_DT            
-- MAGIC FROM
-- MAGIC   ${retailerClient}_retail_alert_im.VW_GET_LOESS_FORECAST_BASELINE_UNIT
-- MAGIC """
-- MAGIC 
-- MAGIC spark.sql(sql)
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC for (date <- GetFutureDates()){
-- MAGIC     println(f"Running for Date - $date")
-- MAGIC     Load_TMP_LOESS_FORECAST_BASELINE_UNIT_HISTORY_SALES_DATA(date)
-- MAGIC     LoessForecast()
-- MAGIC     spark.sql(f"ANALYZE TABLE ${retailerClient}_retail_alert_im.LOESS_FORECAST_BASELINE_UNIT PARTITION(SALES_DT = '${date}') COMPUTE STATISTICS FOR COLUMNS HUB_Organization_Unit_HK, HUB_Retailer_Item_HK")
-- MAGIC     println(f"DONE for Date - $date")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Update the latest LOAD_TS consumed in this batch back to AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS
-- MAGIC 
-- MAGIC This updated LOAD_TS will be used by the next batch to identify dates to forecast

-- COMMAND ----------

INSERT OVERWRITE TABLE ${Retailer_Client}_retail_alert_im.AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS
SELECT
  MAX(LOAD_TS) AS LOAD_TS,
  date_format(current_timestamp, "yyyy-MM-dd hh:mm:ss") AS CREATE_TS
FROM
  ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Drop Temp HOLD table

-- COMMAND ----------

DROP TABLE ${Retailer_Client}_retail_alert_im.TMP_HOLD_AUDIT_LOESS_FORECAST_LAST_PROCESSED_TS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Refresh table LOESS_FORECAST_BASELINE_UNIT

-- COMMAND ----------

REFRESH TABLE ${Retailer_Client}_retail_alert_im.LOESS_FORECAST_BASELINE_UNIT;

-- COMMAND ----------

