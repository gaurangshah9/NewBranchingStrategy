-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### The Data Science process needs a view per Retailer-Client combination. Hence we need to create the views in existing Alert IM Databases per Retailer-Client pointing to BOBv2.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### ParentChainId
-- MAGIC ##### 16 -- Tesco - Parent   
-- MAGIC ##### 23 -- Asda - Parent   
-- MAGIC ##### 24 -- Morrisons - Parent   
-- MAGIC ##### 199 -- Sainsburys - Parent 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CompanyId
-- MAGIC ##### 406	Kraft Heinz Grocery
-- MAGIC ##### 446	Nestle Cereal Partners

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### View Parameters

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Retailer_Client", "sainsburys_nestlecereals", "Retailer_Client")
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "azueus2devadlsdatalake", "ADLS_Account_Name")
-- MAGIC dbutils.widgets.text("countryCode", "uk", "countryCode")
-- MAGIC dbutils.widgets.text("CompanyId", "446", "CompanyId")
-- MAGIC dbutils.widgets.text("ParentChainId", "199", "ParentChainId")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # View vw_gc_ds_input_rsv_measurement

-- COMMAND ----------

CREATE OR REPLACE VIEW ${Retailer_Client}_${countryCode}_retail_alert_im.vw_gc_ds_input_rsv_measurement 
AS
select 
CallfileVisitId,
ParentChainName,
ChainName,
ChainRefExternal,
RefExternal,
InterventionId,
InterventionDate,
InterventionGroup,
Intervention,
DiffStart,
DiffEnd,
Duration
FROM(
select
   cfv.CallfileVisitId as CallfileVisitId,
   COALESCE(pc.FullName,c.FullName) AS ParentChainName, ---If Parent name is null then assign chain name 
   c.FullName AS ChainName,
   o.ChainRefExternal as ChainRefExternal,
   CAST(r.RefExternal AS VARCHAR(1000)) AS RefExternal,
   sir.SurveyItemResponseId as InterventionId,
   sr.VisitDate AS InterventionDate,
   nvl(ivp.InterventionGroup, "") AS InterventionGroup,
   ivp.Intervention as Intervention,
   CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayStart AS VARCHAR(10))) AS DiffStart,
   CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayEnd AS VARCHAR(10))) AS DiffEnd,
   ABS(ivp.DayEnd - ivp.DayStart) + 1 Duration ,  ---Absolute value of Duration
   ROW_NUMBER() OVER (PARTITION BY cfv.CallfileId, o.OutletId, COALESCE(pcm.ProductId, psism.ProductId, 0), sr.VisitDate ORDER BY ivp.InterventionRank) InterventionNo
from
   BOBv2.CallfileVisit cfv 
   INNER JOIN
      BOBv2.vw_BOBv2_Outlet o 
      ON cfv.OutletId = o.OutletId --Join Outlet Bobv2
   inner join
      BOBv2.Chain c 
      on o.ParentChainId = c.ChainId ---Join Chain for ParentChainName
   INNER JOIN
      BOBv2.SurveyResponse sr 
      ON cfv.CallfileVisitId = sr.CallfileVisitId 
   INNER JOIN
      BOBv2.SurveyItemResponse sir 
      ON sr.SurveyResponseId = sir.SurveyResponseId 
   LEFT JOIN
      BOBv2.Chain pc 
      ON c.ParentChainId = pc.ChainId ----Self Join Chain for ChainName 
   INNER JOIN
      BOBv2.SurveyItem si 
      ON sir.SurveyItemId = si.SurveyItemId 
   LEFT JOIN
      BOBv2.SurveyItem psi 
      ON si.ParentSurveyItemId = psi.SurveyItemId 
   INNER JOIN
      bobv2.reachetl_interventions_parameternew ivp 
      ON nvl(sir.SurveyItemOptionId, 0) = ivp.SurveyItemOptionId 
      -----Interventions Join----------------------
      AND 
      (
(si.QuestionVariantId IS NOT NULL 
         AND psi.QuestionVariantId IS NOT NULL 
         AND array_contains(split(ivp.ChildQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true 
         AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(psi.QuestionVariantId AS VARCHAR(100))) = true) 
         OR 
         (
            si.QuestionVariantId IS NOT NULL 
            AND psi.QuestionVariantId IS NULL 
            AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true
         )
      )
   LEFT JOIN
      BOBv2.SurveyItemSubject sis 
      ON sir.SurveyItemSubjectId = sis.SurveyItemSubjectId 
   LEFT JOIN
      BOBv2.ProductControlMap pcm 
      ON sir.ProductControlMapId = pcm.ProductControlMapId 
   LEFT JOIN
      BOBv2.ProductSurveyItemSubjectMap psism 
      ON sis.SurveyItemSubjectId = psism.SurveyItemSubjectId 
      ---------- Join Product for RefExternal------------
   Left join
      (
         SELECT
            p.ProductId,
            COALESCE(c.ParentChainId, c.ChainId) AS ParentChainId,
            RTRIM(LTRIM(REPLACE(pmc.RefExternal, CHAR(9), ''))) AS RefExternal 
         FROM
            BOBv2.Product p 
            INNER JOIN
               BOBv2.ProductMapChain pmc 
               ON p.ProductId = pmc.ProductId 
            INNER JOIN
               BOBv2.Chain c 
               ON pmc.ChainId = c.ChainId 
         WHERE
            p.CompanyId =${CompanyId}
      )
      r 
      ON COALESCE(pcm.ProductId, psism.ProductId, 0) = r.ProductId 
where
   ivp.CompanyId =${CompanyId} ---filter on Each CompanyId 
   AND cfv.IsDeleted = 0 
   and sr.IsDeleted = 0 
   AND sir.IsDeleted = 0 
   and sir.ResponseValue IS NOT NULL
    and o.ParentChainId=${ParentChainId}   ---Filter on Parent chain id 
 ) final 
----Remove duplicate in case of multiple Intervention on InterventionRank 
Where InterventionNo = 1; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Output table ds_rsv_measurement for RSV Measurement 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Drop table if exist

-- COMMAND ----------

DROP TABLE IF EXISTS ${Retailer_Client}_${countryCode}_retail_alert_im.ds_rsv_measurement 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ds_rsv_measurement ddl

-- COMMAND ----------

 CREATE TABLE IF NOT EXISTS  ${Retailer_Client}_${countryCode}_retail_alert_im.ds_rsv_measurement 
   (	
	HUB_RETAILER_ITEM_HK STRING,
	HUB_ORGANIZATION_UNIT_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
	RETAILER_ITEM_ID STRING,
	ORGANIZATION_UNIT_NUM STRING,
	CALLFILE_VISIT_ID INT,
	INTERVENTION_ID  INT,
	SALES_DT DATE,
	INTERVENTION_TYPE STRING,
	INTERVENTION_GROUP_NM STRING,
	POS_ITEM_QTY  DECIMAL(15,2),
	BASELINE_ITEM_QTY DECIMAL(15,2),
	EXPECTED_ITEM_QTY DECIMAL(15,2),
	INTERVENTION_EFFECT FLOAT,
	QINTERVENTION_EFFECT FLOAT,
	DIFF_DAY INT
	)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT, RECORD_SOURCE_CD)
OPTIONS ('compression' 'snappy', path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/informationmart/${Retailer_Client}_${countryCode}_retail_alert/ds_rsv_measurement')
