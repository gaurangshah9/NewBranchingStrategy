// Databricks notebook source
// MAGIC %md
// MAGIC # Create Grouping table for Low Expected Sales And Invalid OSA alert

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


// COMMAND ----------

// MAGIC %md
// MAGIC ### Parameters 
// MAGIC  - grouping_type: could be "grouping_type" low_expected or "invalid_alert"

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.widgets.removeAll()
// MAGIC dbutils.widgets.text("percentile_high", "0.8", "percentile_high")
// MAGIC dbutils.widgets.text("percentile_mid", "0.6", "percentile_mid")
// MAGIC dbutils.widgets.text("minimum_pos", "3", "minimum_pos")
// MAGIC dbutils.widgets.text("week_number", "8", "week_number")
// MAGIC dbutils.widgets.text("group_filter_percentile", "0.8", "group_filter_percentile")
// MAGIC dbutils.widgets.text("min_days_ratio", "0.4", "min_days_ratio")
// MAGIC dbutils.widgets.text("grouping_type", "low_expected", "grouping_type")
// MAGIC dbutils.widgets.text("retailer_client", "asda_nestlecereals", "retailer_client")
// MAGIC dbutils.widgets.text("country_code", "uk", "country_code")
// MAGIC dbutils.widgets.text("record_source_code", "drfe", "record_source_code")

// COMMAND ----------

val retailer_client=dbutils.widgets.get("retailer_client")
val country_code = dbutils.widgets.get("country_code")
val groupingType= dbutils.widgets.get("grouping_type")
val weekNumber = dbutils.widgets.get("week_number").toInt
val minDayRatio = dbutils.widgets.get("min_days_ratio").toDouble

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Get Last 8 weeks data for either baseline pos or pos 
// MAGIC 
// MAGIC - Get past 8 Weeks data from current date based on provided groupingType 
// MAGIC - if groupingType == low_expected,  BASELINE_POS_DATA  
// MAGIC - if groupingType == invalid_alert, POS_DATA
// MAGIC - Remove data with Negative POS

// COMMAND ----------

def getPOSData(groupingType:String): DataFrame = {
  val posData = spark.sql(
    s"""
    select 
      ORGANIZATION_UNIT_NUM,
      RETAILER_ITEM_ID,
      SALES_DT, 
      o.HUB_ORGANIZATION_UNIT_HK,
      h.HUB_RETAILER_ITEM_HK, 
      POS_ITEM_QTY
    from 
       ${retailer_client}_${country_code}_dv.vw_sat_link_epos_summary  s
    join 
       ${retailer_client}_${country_code}_dv.hub_retailer_item h
    on 
      h.HUB_RETAILER_ITEM_HK=s.HUB_RETAILER_ITEM_HK
    join 
       ${retailer_client}_${country_code}_dv.hub_organization_unit o
    on 
      o.HUB_ORGANIZATION_UNIT_HK=s.HUB_ORGANIZATION_UNIT_HK
    """
    ) 
  
    val baselinePosData = spark.sql(
    s"""
    select 
      ORGANIZATION_UNIT_NUM,
      RETAILER_ITEM_ID,
      SALES_DT, 
      o.HUB_ORGANIZATION_UNIT_HK,
      h.HUB_RETAILER_ITEM_HK, 
      BASELINE_POS_ITEM_QTY as POS_ITEM_QTY
    from 
       ${retailer_client}_${country_code}_retail_alert_im.drfe_forecast_baseline_unit s
    join 
       ${retailer_client}_${country_code}_dv.hub_retailer_item h
    on 
      h.HUB_RETAILER_ITEM_HK=s.HUB_RETAILER_ITEM_HK
    join 
       ${retailer_client}_${country_code}_dv.hub_organization_unit o
    on 
      o.HUB_ORGANIZATION_UNIT_HK=s.HUB_ORGANIZATION_UNIT_HK
    """
    ) 
  
  var salesData = if(groupingType.toUpperCase == "INVALID_ALERT") posData else baselinePosData
  val filteredPOSData =  salesData.filter($"SALES_DT".gt(lit(date_sub(current_date(), weekNumber*7)))).
                                   filter($"SALES_DT".leq(lit(current_date())))

  return filteredPOSData.filter($"POS_ITEM_QTY" >= lit(0))
}

// COMMAND ----------

def getTargetTableName(groupingType: String):String = { 
  val databaseName=s"${retailer_client}_${country_code}_retail_alert_im"
  val targetTableName =  if(groupingType.toUpperCase == "INVALID_ALERT") 
                              s"${retailer_client}_${country_code}_retail_alert_im.alert_osa_invalid_alert_grouping" 
                          else 
                              s"${retailer_client}_${country_code}_retail_alert_im.alert_osa_low_expected_sale_grouping"
  return targetTableName
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### Daily ADD Group Function 
// MAGIC  - for each store for all the 8 weeks and all the stores it find following values 
// MAGIC    - 80 percentile as TOP_THRESHOLD_QTY
// MAGIC    - 60 percentile as MID_THRESHOLD_QTY
// MAGIC  -  it applies following filters/rules for grouping 
// MAGIC 
// MAGIC |	CONDITION 					| GROUP_NM  |
// MAGIC |-------------------------------|-----------|
// MAGIC |	MID_THRESHOLD_QTY < minimum_pos   	| slow 		|
// MAGIC | 	POS_QTY < MID_THRESHOLD_QTY | slow 		|
// MAGIC |	TOP_THRESHOLD_QTY <= POS_QTY| Mid  		|
// MAGIC |	POS_QTY > TOP_THRESHOLD_QTY	| Top 		| 

// COMMAND ----------

def getGroupsPerItem(filterdSalesData: DataFrame): DataFrame = { 

  val col_name = "pos_item_qty"
  val minimumPOS= dbutils.widgets.get("minimum_pos").toInt
  val percentileHigh =  dbutils.widgets.get("percentile_high").toDouble
  val percentileMid =  dbutils.widgets.get("percentile_mid").toDouble

  val dataWithTopThreshold =  filterdSalesData.
                                    withColumn("TOP_THRESHOLD_QTY", callUDF("percentile_approx",col(col_name),lit(percentileHigh)) over Window.partitionBy($"retailer_item_id")).
                                    withColumn("MID_THRESHOLD_QTY", callUDF("percentile_approx",col(col_name),lit(percentileMid)) over Window.partitionBy($"retailer_item_id")).
                                    withColumn("MINIMUM_POS_ITEM_QTY", lit(minimumPOS))


  val groupedData = dataWithTopThreshold.
                        withColumn("GROUP_NM", when($"MID_THRESHOLD_QTY" < $"MINIMUM_POS_ITEM_QTY", "slow").
                                     otherwise(
                                           when($"POS_ITEM_QTY" > $"MID_THRESHOLD_QTY" && $"POS_ITEM_QTY" <= $"TOP_THRESHOLD_QTY", "middle").
                                           when($"POS_ITEM_QTY" > $"TOP_THRESHOLD_QTY", "top" ).
                                           when($"POS_ITEM_QTY" <= $"MID_THRESHOLD_QTY", "slow")
                                     )
                                  )
  return groupedData
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ### select percentile combination 
// MAGIC 
// MAGIC  - calcualte the number of days item,store,group combination has appeard 
// MAGIC  - derive MEMBERSHIP_THRESHOLD_NUM as 80 percentile of days based on item, group combination 
// MAGIC  - derive minimum days as 40 percentage of total days 
// MAGIC  - apply following filters 
// MAGIC   - DAYS_IN_GROUP_NUM >= MEMBERSHIP_THRESHOLD_NUM
// MAGIC   - MEMBERSHIP_THRESHOLD_NUM >= MINIMUM_DAYS

// COMMAND ----------

def getFiltertedGropups(groupedData: DataFrame): DataFrame = { 

  val groupFilterPercentile = dbutils.widgets.get("group_filter_percentile")
  val grouppedDays = groupedData.
                        filter($"GROUP_NM" =!= "slow").
                        withColumn("DAYS_IN_GROUP_NUM", count($"SALES_DT") over Window.partitionBy($"retailer_item_id", $"organization_unit_num", $"GROUP_NM")).
                        drop($"SALES_DT").
                        drop($"POS_ITEM_QTY").
                        dropDuplicates
  val withThreshold = grouppedDays.
                        withColumn("MEMBERSHIP_THRESHOLD_NUM", callUDF("percentile_approx",col("DAYS_IN_GROUP_NUM"),lit(groupFilterPercentile)) over Window.partitionBy($"retailer_item_id", $"GROUP_NM")).
                        withColumn("MINIMUM_DAYS", lit(weekNumber*7)*lit(minDayRatio))
  val filteredGroups = withThreshold.
                          filter($"DAYS_IN_GROUP_NUM" >= $"MEMBERSHIP_THRESHOLD_NUM").
                          filter($"MEMBERSHIP_THRESHOLD_NUM" >= $"MINIMUM_DAYS")
  return filteredGroups
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Load grouping data into tables. 

// COMMAND ----------

def loadGroupingDataIntoTables(filteredGroups:DataFrame, targetTableName:String): Unit = { 
  val recordSourceCode=dbutils.widgets.get("record_source_code")
  filteredGroups.
    withColumn("RECORD_SOURCE_CODE", lit(recordSourceCode)).
    withColumn("LOAD_TS", lit(current_timestamp())).
    select("ORGANIZATION_UNIT_NUM",
          "RETAILER_ITEM_ID",
          "HUB_ORGANIZATION_UNIT_HK",
          "HUB_RETAILER_ITEM_HK",
          "MINIMUM_POS_ITEM_QTY",
          "MID_THRESHOLD_QTY",
          "TOP_THRESHOLD_QTY",
          "LOAD_TS",
          "DAYS_IN_GROUP_NUM",
          "MEMBERSHIP_THRESHOLD_NUM",
          "RECORD_SOURCE_CODE",
          "GROUP_NM"
          ).
    distinct.
    write.
//       mode("overwrite").
      insertInto(s"${targetTableName}")

}

// COMMAND ----------

def removeData(tableName: String): Unit = { 
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)
  spark.sql(s"delete from ${tableName}")
  spark.sql(s"vacuum ${tableName} RETAIN 0 HOURS")
  spark.sql(s"fsck repair table ${tableName}")
   
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Code Entry Point

// COMMAND ----------

val groupingType = dbutils.widgets.get("grouping_type")
val posData = getPOSData(groupingType)
val groupedData = getGroupsPerItem(posData)
val filteredGroups = getFiltertedGropups(groupedData)
val targetTableName = getTargetTableName(groupingType)
removeData(targetTableName)
loadGroupingDataIntoTables(filteredGroups, targetTableName)
