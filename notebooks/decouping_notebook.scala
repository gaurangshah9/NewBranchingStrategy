// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


// COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("retailer_client", "asda_nestlecereals", "retailer_client")
dbutils.widgets.text("country_code", "uk", "country_code")
dbutils.widgets.text("record_source_code", "drfe", "record_source_code")
dbutils.widgets.text("membership_multipler", "1", "membership_multipler")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Initialize variables

// COMMAND ----------

val retailer_client=dbutils.widgets.get("retailer_client")
val country_code = dbutils.widgets.get("country_code")
val retailerClient = dbutils.widgets.get("retailer_client")
val recordSourceCode = dbutils.widgets.get("record_source_code")
val countryCode  = dbutils.widgets.get("country_code")
val membershipMultipler = dbutils.widgets.get("membership_multipler").toDouble
val martDatabaseName = s"${retailer_client}_${country_code}_retail_alert_im"
val alertTableName = s"${martDatabaseName}.alert_osa_low_expected_sale"
val currentSalesDateTable = s"${martDatabaseName}.tmp_hold_audit_${recordSourceCode}_alerts_osa_low_expected_sale_last_processed_ts"
val processedSalesDateTable= s"${martDatabaseName}.audit_${recordSourceCode}_alerts_osa_low_expected_sale_last_processed_ts"

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Get Sales Dates to process

// COMMAND ----------

/**
   *  if table name does not contain any records notebook will fail with exception 
   *  
   * @param tableName table name to verify 
   */
  def tableShouldContainValue(tableName: String) : Unit = {
    val df = spark.sql(f"SELECT * FROM ${tableName}")
    val count = df.count
    println(count)
    if (count == 0 ){
      throw new Exception(s"${tableName} table has no records")
    }
  }

  /**
   * Creates temporary table with sales data to process based on audit_driver_sales_dates and 
   * audit_${recordSourceCode}_alerts_osa_low_expected_sale_last_processed_ts table 
   */
  def createCurrentSalesDateTable(): Unit = {
    val dropTable = s"""DROP TABLE IF EXISTS ${currentSalesDateTable}"""
    val createTable= s"""
  CREATE TABLE ${currentSalesDateTable} AS
  SELECT
    SALES_DT,
    LOAD_TS
    FROM
      (
        SELECT
          a.SALES_DT,
          a.LOAD_TS,
          ROW_NUMBER() OVER(PARTITION BY a.SALES_DT ORDER BY a.LOAD_TS DESC) rnum
        FROM
          ${retailerClient}_${countryCode}_dv.audit_driver_sales_dates a
        INNER JOIN
          ${processedSalesDateTable} b
        ON
          a.LOAD_TS > b.LOAD_TS
      ) a
   WHERE
    rnum = 1
  """
    spark.sql(dropTable)
    spark.sql(createTable)

  }

  /**
   * overwrites the audit_${recordSourceCode}_alerts_osa_low_expected_sale_last_processed_ts
   * table with processed sales dates
   */
  def updateProcessedSalesDateTable(): Unit = {
    val updateProcessedDateTable = s"""INSERT OVERWRITE TABLE ${processedSalesDateTable}
  SELECT
    MAX(LOAD_TS) AS LOAD_TS,
    date_format(current_timestamp, "yyyy-MM-dd hh:mm:ss") AS CREATE_TS
  FROM ${currentSalesDateTable}"""

    spark.sql(updateProcessedDateTable)
  }

  /**
   *
   * @return returns sales dates to process sperated by command (i.e. 2020-01-01, 2020-01-02)
   */
  def getSalesDateToProcess(): String = {
    tableShouldContainValue(processedSalesDateTable)
    createCurrentSalesDateTable()
    tableShouldContainValue(currentSalesDateTable)
    return spark.sql(s"SELECT * FROM ${currentSalesDateTable}").collect.map("'" + _(0) + "'").mkString(",")

  }

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Get POS Data based on given Sales Dates

// COMMAND ----------

def getBaselinePOSData(salesDates: String): DataFrame = {
  val sql = s"""
  select 
    org.ORGANIZATION_UNIT_NUM,
    item.RETAILER_ITEM_ID,
    forecast.SALES_DT, 
    UNIT_PRICE_AMT,
    org.HUB_ORGANIZATION_UNIT_HK,
    item.HUB_RETAILER_ITEM_HK, 
    BASELINE_POS_ITEM_QTY,
    GROUP_NM
    from 
      ${retailerClient}_${countryCode}_retail_alert_im.drfe_forecast_baseline_unit forecast
    join 
      ${retailerClient}_${countryCode}_dv.hub_retailer_item item
    on 
      item.HUB_RETAILER_ITEM_HK=forecast.HUB_RETAILER_ITEM_HK
    join 
      ${retailerClient}_${countryCode}_dv.hub_organization_unit org
    on 
      org.HUB_ORGANIZATION_UNIT_HK=forecast.HUB_ORGANIZATION_UNIT_HK
    join 
      ${retailerClient}_${countryCode}_dv.vw_sat_retailer_item_unit_price price 
    on 
      price.HUB_ORGANIZATION_UNIT_HK = org.HUB_ORGANIZATION_UNIT_HK
    and
      price.HUB_RETAILER_ITEM_HK = item.HUB_RETAILER_ITEM_HK
    and
      price.sales_dt = forecast.sales_dt
    join
      ${retailerClient}_${countryCode}_retail_alert_im.alert_osa_low_expected_sale_grouping grp
    on
      grp.ORGANIZATION_UNIT_NUM = org.ORGANIZATION_UNIT_NUM
    and 
      grp.RETAILER_ITEM_ID = item.RETAILER_ITEM_ID
      WHERE forecast.sales_dt in (${salesDates})
   """
  return spark.sql(sql)
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Generate Alert
// MAGIC 
// MAGIC - for every single day:
// MAGIC  - calculate Q1(0.25 quantile),  and Q3(0.75quantile) of expected_pos of all stores in "high" or "mid" Selling Group
// MAGIC  - filter records if BASELINE_POS_ITEM_QTY of store <= Q1 - MEMBERSHIP_MULTIPLIER * (Q3-Q1)
// MAGIC  - generate alert and return (Q2 - BASELINE_POS_ITEM_QTY) * UNIT_PRICE_AMT as lost sales value

// COMMAND ----------

def generateAlerts(df:DataFrame, membershipMultiplier: Double): DataFrame = { 
  val colName = "BASELINE_POS_ITEM_QTY"
  val window = Window.partitionBy($"RETAILER_ITEM_ID", $"GROUP_NM", $"SALES_DT")
  val dfWithQ3Q1Diff = df.
                        withColumn("Q3", callUDF("percentile_approx",col(colName),lit(0.75)) over(window)).
                        withColumn("Q2", callUDF("percentile_approx",col(colName),lit(0.50)) over(window)).
                        withColumn("Q1", callUDF("percentile_approx",col(colName),lit(0.25)) over(window)).
                        withColumn("OUTLIER_THRESHOLD", $"Q1" - (($"Q3"-$"Q1")* membershipMultiplier))
  
  val lowSalesAlert = dfWithQ3Q1Diff.
                        filter(col(colName) <=$"OUTLIER_THRESHOLD").
                        withColumn("LOST_SALES_AMT", ($"Q2"-col(colName)) * $"UNIT_PRICE_AMT")
  
  return lowSalesAlert
  
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Get On Hand Inventory for given store, item, sales date combination

// COMMAND ----------

def getOnHandInventory(lowExpectedAlerts: DataFrame, salesDates: String) : DataFrame = { 
  val satLinkEPOSSummary = spark.sql(s"select * from ${retailerClient}_${countryCode}_dv.vw_sat_link_epos_summary where SALES_DT IN (${salesDates})")
  val AlertWithInventory = lowExpectedAlerts.join(satLinkEPOSSummary , Seq("HUB_ORGANIZATION_UNIT_HK", "HUB_RETAILER_ITEM_HK", "SALES_DT"))
                                            .select(
                                                "HUB_ORGANIZATION_UNIT_HK",
                                                "ORGANIZATION_UNIT_NUM",
                                                "HUB_RETAILER_ITEM_HK",
                                                "RETAILER_ITEM_ID",
                                                "RECORD_SOURCE_CD",
                                                "ON_HAND_INVENTORY_QTY",
                                                "LOST_SALES_AMT",
                                                "SALES_DT"
                                              )
  return AlertWithInventory
    
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Load Alerts into alert_osa_low_expected_sale table 

// COMMAND ----------

def loadAlertTable(lowExpectedAlerts:DataFrame, targetTableName:String): Unit = {
  val recordSourceCode=dbutils.widgets.get("record_source_code")
  lowExpectedAlerts.
    withColumn("RECORD_SOURCE_CD", lit(recordSourceCode)).
    withColumn("LOAD_TS", lit(current_timestamp())).
    withColumn("ALERT_MESSAGE_DESC", lit("low expected sales")).
    withColumn("ALERT_TYPE_NM", lit("LowExpectedSales")).
    select(
          "HUB_ORGANIZATION_UNIT_HK",
          "HUB_RETAILER_ITEM_HK",
          "ORGANIZATION_UNIT_NUM",
          "RETAILER_ITEM_ID",
          "ON_HAND_INVENTORY_QTY",
          "LOAD_TS",
          "ALERT_MESSAGE_DESC", 
          "ALERT_TYPE_NM",
          "LOST_SALES_AMT",
          "SALES_DT",
          "RECORD_SOURCE_CD"
          ).
    distinct.
    write.
    mode("overwrite").
    insertInto(targetTableName)
    
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Main Func

// COMMAND ----------

val salesDates = getSalesDateToProcess() 
val posDatawithGroups = getBaselinePOSData(salesDates)
val lowExpectedAlerts = generateAlerts(posDatawithGroups, membershipMultipler)
val lowExepctedAlertsWithOnHandInventory = getOnHandInventory(lowExpectedAlerts, salesDates)
loadAlertTable(lowExepctedAlertsWithOnHandInventory, alertTableName)
updateProcessedSalesDateTable()
