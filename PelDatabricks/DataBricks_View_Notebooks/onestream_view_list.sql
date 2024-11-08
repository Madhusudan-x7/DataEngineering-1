-- Databricks notebook source
CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_EBS_RESOPS_TB_ACTUAL_V 
AS SELECT 
'Actual' SCENARIO,
period_name,
CONCAT('FY',substring(period_name,5,4)) FISCAL_YEAR,
CONCAT('FM',CASE substring(period_name,1,3) WHEN 'DEC' THEN '01' WHEN 'JAN' THEN '02' WHEN 'FEB' THEN '03' WHEN 'MAR' THEN '04' WHEN 'APR' THEN '05' WHEN 'MAY' THEN '06' WHEN 'JUN' THEN '07' WHEN 'JUL' THEN '08' WHEN 'AUG' THEN '09' WHEN 'SEP' THEN '10' WHEN 'OCT' THEN '11' WHEN 'NOV' THEN '12' END) FISCAL_MONTH,
concat(concat(gcc.segment1,"_"),gcc.segment2) Entity,
gcc.segment4 ACCOUNT, 
ffvt.description ,         
concat('CC',gcc.segment3) COST_CENTER, 
gcc.segment5 SEGMENT, 
'' ICP,
(gb.period_net_dr - gb.period_net_cr) + (gb.begin_balance_dr - gb.begin_balance_cr) AMOUNT      
FROM silver.oraerpdb_erpprod_gl_gl_balances gb 
      inner join silver.oraerpdb_erpprod_gl_gl_code_combinations gcc  
        on gb.code_combination_id = gcc.code_combination_id 
      inner join silver.oraerpdb_erpprod_apps_fnd_flex_values ffv 
        on gcc.segment4= ffv.flex_value and  ffv.flex_value_set_id = 1002672
      inner join silver.oraerpdb_erpprod_apps_fnd_flex_values_tl ffvt on  ffv.flex_value_id = ffvt.flex_value_id and ffvt.language = 'US'--added by SB    
WHERE gb.actual_flag = 'A' 
    AND ( (EXISTS (SELECT 'x' --ffvv.flex_value 
                      FROM silver.oraerpdb_erpprod_apps_fnd_flex_values_vl ffvv, 
                           silver.oraerpdb_erpprod_applsys_fnd_flex_value_sets ffvs 
                     WHERE ffvv.flex_value_set_id = ffvs.flex_value_set_id 
                       AND ffvs.flex_value_set_name = 'PEL_FDMEE_CAN_ODI_A' 
                       AND ffvv.enabled_flag = 'Y' 
                       AND ffvv.flex_value = gcc.segment1) 
               AND gb.currency_code = 'CAD') 
       OR (EXISTS (SELECT 'x' --ffvv.flex_value 
                     FROM silver.oraerpdb_erpprod_apps_fnd_flex_values_vl ffvv, 
                          silver.oraerpdb_erpprod_applsys_fnd_flex_value_sets ffvs 
                    WHERE ffvv.flex_value_set_id = ffvs.flex_value_set_id 
                      AND ffvs.flex_value_set_name = 'PEL_FDMEE_CORP_ODI_A' 
                      AND ffvv.enabled_flag = 'Y' 
                      AND ffvv.flex_value = gcc.segment1) 
              AND gb.currency_code = 'USD')) 
     AND (gb.period_net_dr - gb.period_net_cr) + (gb.begin_balance_dr - gb.begin_balance_cr) <> 0 
ORDER BY gb.period_name, 
         gb.actual_flag, 
         gcc.segment1, 
         gcc.segment2, 
        gcc.segment3, 
         gcc.segment4, 
         gcc.segment5


-- COMMAND ----------



-- COMMAND ----------

CREATE or replace  VIEW silver.pel_onestream_edw_agg_fcpl_v 
AS SELECT
'ACTUAL' SCENARIO,
EVNT_TYPE,

/*DATE FIELDS*/
DATE_UKEY EVNT_DTM,
DECODE(TRANSACTION_DAY_NAME,'Monday','MON','Tuesday','TUE','Wednesday','WED','Thursday','THU','Friday','FRI','Saturday','SAT','Sunday','SUN') WEEK_DAY,
CONCAT('FW',LPAD(CAST(DATE_DIM.FISCAL_WK_OF_YR AS INTEGER) ,2,'0')) FISCAL_WEEK,
CONCAT('FY',CAST(DATE_DIM.FISCAL_YR_NUM AS INTEGER)) FISCAL_YEAR,
CONCAT(CONCAT(CAST(FISCAL_MONTH_OF_YR AS INT),"/"),CAST(FISCAL_YR_NUM AS INT)) FM_FY,
FISCAL_YR_MONTH,
FISCAL_YR_NUM, 
FISCAL_YR_WK,
FISCAL_MONTH_OF_YR,
EVNT_DTM_SKEY,
RPTD_DIM.HDR_SKEY,
RPTD_DIM.RPTD_SKEY,
/*Dimnesion Attributes */
RPTD_DIM.ORG_STRUCTURE BUSINESS_STRUCTURE,
CASE WHEN REF_PLNT_NO_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_NO_BRAND.REV_ORG_CODE WHEN REF_PLNT_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_BRAND.REV_ORG_CODE ELSE NVL(PLNT_DIM.PLANT_CODE,'NO CODE FOUND') END ORG_CODE,
PLNT_DIM.PLANT_CODE ORIG_PLANT_CODE ,
NVL(SSEG_DIM.SALES_SEGMENT,'NO SALES SEGMENT') SALES_SEGMENT,
CASE WHEN RPTD_FCPL_DIM.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL_DIM.FP_SUB_CHANNEL END FP_SUB_CHANNEL,
RPTD_FCPL_DIM.FP_ORDER_TYPE,
CASE WHEN RPTD_FCPL_DIM.FP_BRAND_SERIES IN ('N/A','OTHER','UNKNOWN') THEN RPTD_FCPL_DIM.FP_BRAND_SERIES || CASE RPTD_FCPL_DIM.ORG_STRUCTURE  WHEN 'WOOD WINDOWS AND PATIO DOORS' THEN ' WOOD' WHEN 'VINYL WINDOWS AND PATIO DOORS' THEN ' VINYL' WHEN 'FIBERGLASS WINDOWS AND PATIO DOORS' THEN ' FIBERGLASS' ELSE CONCAT(' ',RPTD_FCPL_DIM.ORG_STRUCTURE) END ELSE RPTD_FCPL_DIM.FP_BRAND_SERIES END FP_BRAND_SERIES,
PKDG_FCPL_DIM.FP_PRODUCT,
NVL(PROFISEE_DATA_PRODUCT_FAMILY.NAME,'UNKNOWN PRODUCT FAMILY') PRODUCT_FAMILY,
PKDG_FCPL_DIM.FP_EXT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_GRILLE,
PKDG_FCPL_DIM.FP_INT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_SCREEN,
PKDG_FCPL_DIM.FP_TWO_COLOR,
CHAN_DIM.CLIENT_CODE BRANCH_ACCOUNT_CODE,


/*PROFIEE CODES*/
PROFISEE_DATA_BUSINESS_STRUCTURE.CODE BUSINESS_STRUCTURE_CODE,

PROFISEE_DATA_SALES_SEGMENT.CODE SALES_SEGMENT_CODE,
PROFISEE_DATA_SALES_SEGMENT.MEMBER SALES_SEGMENT_MEMBER_CODE,  
PROFISEE_DATA_SALES_SEGMENT.MEMBER_DESCRIPTION SALES_SEGMENT_MEMBER_DESC,  

PROFISEE_DATA_SEGMENT_BUILDING_TYPE.CODE SEGMENT_BUILDING_TYPE_CODE,
PROFISEE_DATA_SEGMENT_BUILDING_TYPE.MEMBER SEGMENT_BUILDING_TYPE_MEMBER_CODE,
PROFISEE_DATA_SEGMENT_BUILDING_TYPE.MEMBER_DESCRIPTION SEGMENT_BUILDING_TYPE_DESC,

PROFISEE_DATA_SEGMENT.CODE SEGMENT_CODE,
PROFISEE_DATA_SEGMENT.MEMBER SEGMENT_MEMBER_CODE,  
PROFISEE_DATA_SEGMENT.MEMBER_DESCRIPTION SEGMENT_MEMBER_DESC,  

PROFISEE_DATA_BRAND_SERIES.CODE BRAND_SERIES_CODE,
PROFISEE_DATA_BRAND_SERIES.MEMBER  BRAND_SERIES_MEMBER_CODE,
PROFISEE_DATA_BRAND_SERIES.MEMBER_DESCRIPTION  BRAND_SERIES_MEMBER_DESC,

PROFISEE_DATA_PRODUCT.CODE  FP_PRODUCT_CODE  ,
PROFISEE_DATA_PRODUCT.MEMBER FP_PRODUCT_MEMBER_CODE   ,
PROFISEE_DATA_PRODUCT.MEMBER_DESCRIPTION FP_PRODUCT_MEMBER_DESC  ,

PROFISEE_DATA_PRODUCT_FAMILY.CODE PRODUCT_FAMILY_CODE,
PROFISEE_DATA_PRODUCT_FAMILY.MEMBER PRODUCT_FAMILY_MEMBER_CODE,
PROFISEE_DATA_PRODUCT_FAMILY.MEMBER_DESCRIPTION PRODUCT_FAMILY_MEMBER_DESC,

PROFISEE_DATA_FP_SUB_CHANNEL.CODE FP_SUB_CHANNEL_CODE,
PROFISEE_DATA_FP_SUB_CHANNEL.MEMBER FP_SUB_CHANNEL_MEMBER_CODE,
PROFISEE_DATA_FP_SUB_CHANNEL.MEMBER_DESCRIPTION FP_SUB_CHANNEL_MEMBER_DESC,


PROFISEE_DATA_BRANCH_ACCOUNT_CODE.CODE BRANCH_ACCOUNT_CODE_CODE,
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.MEMBER BRANCH_ACCOUNT_CODE_MEMBER_CODE,
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.MEMBER_DESCRIPTION BRANCH_ACCOUNT_CODE_MEMBER_DESC,    

PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.CODE BRANCH_ACCOUNT_NUMBER_CODE,
PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.MEMBER BRANCH_ACCOUNT_NUMBER_MEMBER_CODE,
PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.MEMBER_DESCRIPTION BRANCH_ACCOUNT_NUMBER_MEMBER_DESC,    



case when REF_PLNT_NO_BRAND.mfg_Org_Code is not null  then REF_PLNT_NO_BRAND.Revenue_Organization when REF_PLNT_BRAND.mfg_org_code is not null then REF_PLNT_BRAND.Revenue_Organization else nvl(org.Code,'NO CODE FOUND') end Organization_Code,


/*Measures */
SUM(GROSS_AMT) EXT_GROSS_AMT,
SUM(PURCHASE_QTY) PURCHASE_QTY,
CAST(SUM(DOOR_PANEL_QTY) AS INTEGER) DOOR_PANEL_QTY,
SUM(MATL_COST_AMT) MATERIAL_COST_AMT,
CAST(SUM(CASE WHEN PKDG_FCPL_DIM.FP_UNIT_TYPE = 'COMPLETE UNIT' THEN PURCHASE_QTY ELSE 0 END) AS INTEGER) COMPLETE_UNIT_QTY,
SUM(CASE WHEN PKDG_FCPL_DIM.FP_UNIT_TYPE = 'COMPLETE UNIT' THEN GROSS_AMT ELSE 0 END)  COMPLETE_UNIT_EXT_GROSS_AMT,
0  EXT_SELLING_PRICE_AMT,
0 PRICE_AMT,
(SELECT MAX(SUBSTRING(INS_DTM,1,10)) FROM SILVER.EDWPRODDB_EDWPROD_FACT_AGG_FCPL_DA_FACT MAX_DTM) DL_INS_DTM,

concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(
concat(PROFISEE_DATA_BUSINESS_STRUCTURE.CODE,'|'),
PROFISEE_DATA_BRAND_SERIES.CODE),'|'),
PROFISEE_DATA_PRODUCT.CODE),'|'),
case when REF_PLNT_NO_BRAND.mfg_Org_Code is not null  then REF_PLNT_NO_BRAND.Revenue_Organization when REF_PLNT_BRAND.mfg_org_code is not null then REF_PLNT_BRAND.Revenue_Organization else nvl(org.Code,'NO CODE FOUND') end ),'|'),
PROFISEE_DATA_FP_SUB_CHANNEL.CODE),'|'),
PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.CODE),'|'),
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.CODE),'|'),
PROFISEE_DATA_SEGMENT.CODE),'|'),
PROFISEE_DATA_SEGMENT_BUILDING_TYPE.CODE),'|'),
PROFISEE_DATA_SALES_SEGMENT.CODE),'|'),
FP_ORDER_TYPE) FU_KEY

FROM 
SILVER.EDWPRODDB_EDWPROD_FACT_AGG_FCPL_DA_FACT  AGG_FCPL_DA_FACT 
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_EVNT_DIM EVNT_DIM  ON AGG_FCPL_DA_FACT.EVNT_SKEY = EVNT_DIM.EVNT_SKEY 
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_DATE_DIM DATE_DIM  ON AGG_FCPL_DA_FACT.EVNT_DTM_SKEY = DATE_DIM.DATE_SKEY 
   LEFT OUTER JOIN SILVER.EDWPRODDB_EDWPROD_EDW_EXT_PROFISEE_DOMAIN_SSEG_V  SSEG_DIM ON AGG_FCPL_DA_FACT.SSEG_SKEY = SSEG_DIM.SSEG_SKEY                                  
   LEFT OUTER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_CHAN_DIM CHAN_DIM ON AGG_FCPL_DA_FACT.CHAN_SKEY = CHAN_DIM.CHAN_SKEY  
   INNER JOIN  SILVER.EDWPRODDB_EDWPROD_DIM_RPTD_DIM RPTD_DIM ON AGG_FCPL_DA_FACT.RPTD_SKEY = RPTD_DIM.RPTD_SKEY
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_RPTD_FCPL_DIM RPTD_FCPL_DIM ON RPTD_DIM.RPTD_FCPL_SKEY = RPTD_FCPL_DIM.RPTD_FCPL_SKEY  
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_PLNT_DIM  PLNT_DIM ON RPTD_DIM.PLNT_SKEY = PLNT_DIM.PLNT_SKEY AND PLNT_DIM.PLANT_CODE NOT IN ('COL')   
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_PKDG_FCPL_DIM  PKDG_FCPL_DIM ON AGG_FCPL_DA_FACT.PKDG_FCPL_SKEY = PKDG_FCPL_DIM.PKDG_FCPL_SKEY  AND PKDG_FCPL_DIM.FP_PRODUCT NOT IN ( 'STORM DOOR')
   
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_SALES_SEGMENT  ON   NVL(SSEG_DIM.SALES_SEGMENT,'NO SALES SEGMENT DATA') = PROFISEE_DATA_SALES_SEGMENT.NAME
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_SEGMENT_BUILDING_TYPE   ON PROFISEE_DATA_SALES_SEGMENT.SEGMENT_BUILDING_TYPE =  PROFISEE_DATA_SEGMENT_BUILDING_TYPE.CODE
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_SEGMENT  ON PROFISEE_DATA_SEGMENT_BUILDING_TYPE.SEGMENT = PROFISEE_DATA_SEGMENT.CODE
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_BRANCH_ACCOUNT_CODE  ON CHAN_DIM.CLIENT_CODE = PROFISEE_DATA_BRANCH_ACCOUNT_CODE.SOURCE_ID
   
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER  ON PROFISEE_DATA_BRANCH_ACCOUNT_CODE.Branch_Account_Number = PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.CODE
   
   LEFT OUTER JOIN  SILVER.PROFISEE_DATA_FP_SUB_CHANNEL  ON CASE WHEN RPTD_FCPL_DIM.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL_DIM.FP_SUB_CHANNEL END  = PROFISEE_DATA_FP_SUB_CHANNEL.NAME  
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_BUSINESS_STRUCTURE  ON RPTD_DIM.ORG_STRUCTURE = PROFISEE_DATA_BUSINESS_STRUCTURE.NAME
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_BRAND_SERIES  ON CASE WHEN RPTD_FCPL_DIM.FP_BRAND_SERIES IN ('N/A','OTHER','UNKNOWN') THEN RPTD_FCPL_DIM.FP_BRAND_SERIES || CASE RPTD_FCPL_DIM.ORG_STRUCTURE  WHEN 'WOOD WINDOWS AND PATIO DOORS' THEN ' WOOD' WHEN 'VINYL WINDOWS AND PATIO DOORS' THEN ' VINYL' WHEN 'FIBERGLASS WINDOWS AND PATIO DOORS' THEN ' FIBERGLASS' ELSE CONCAT(' ',RPTD_FCPL_DIM.ORG_STRUCTURE) END ELSE RPTD_FCPL_DIM.FP_BRAND_SERIES END   = PROFISEE_DATA_BRAND_SERIES.NAME
   LEFT OUTER JOIN  SILVER.PROFISEE_DATA_PRODUCT ON PKDG_FCPL_DIM.FP_PRODUCT = PROFISEE_DATA_PRODUCT.SOURCE_ID  
   LEFT OUTER JOIN  SILVER.PROFISEE_DATA_PRODUCT_FAMILY ON PROFISEE_DATA_PRODUCT.PRODUCT_FAMILY = PROFISEE_DATA_PRODUCT_FAMILY.CODE
   
   
   LEFT OUTER JOIN silver.profisee_data_organization org ON CASE
                                                          
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SPDEPOT' THEN 'P02'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SHENANDO' THEN 'S05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'PELLCASE' THEN 'P05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'ACPLANT' THEN 'P07'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SIOUXCTR' THEN 'S12'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SUPPORT' THEN 'P08'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'DOOR' THEN 'P06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'METAL' THEN 'P10'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'CARROLL' THEN 'C10'
                                                            ELSE PLNT_DIM.PLANT_CODE
                                                            END = org.org_code
     
   
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_REVENUE_PLANT_MOVES REF_PLNT_NO_BRAND ON CASE
                                                          
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SPDEPOT' THEN 'P02'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SHENANDO' THEN 'S05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'PELLCASE' THEN 'P05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'ACPLANT' THEN 'P07'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SIOUXCTR' THEN 'S12'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SUPPORT' THEN 'P08'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'DOOR' THEN 'P06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'METAL' THEN 'P10'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'CARROLL' THEN 'C10'
                                                            ELSE PLNT_DIM.PLANT_CODE
                                                            END = REF_PLNT_NO_BRAND.MFG_ORG_CODE AND REF_PLNT_NO_BRAND.`MANUFACTURING_BRAND_SERIES.NAME` IS  NULL

   LEFT OUTER JOIN SILVER.PROFISEE_DATA_REVENUE_PLANT_MOVES  REF_PLNT_BRAND ON CASE
                                                            
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SPDEPOT' THEN 'P02'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SHENANDO' THEN 'S05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'PELLCASE' THEN 'P05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'ACPLANT' THEN 'P07'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SIOUXCTR' THEN 'S12'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SUPPORT' THEN 'P08'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'DOOR' THEN 'P06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'METAL' THEN 'P10'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'CARROLL' THEN 'C10'
                                                            ELSE PLNT_DIM.PLANT_CODE
                                                            END = REF_PLNT_BRAND.MFG_ORG_CODE AND RPTD_FCPL_DIM.FP_BRAND_SERIES = REF_PLNT_BRAND.`MANUFACTURING_BRAND_SERIES.NAME`
                                                            
                                                            
                                                            
  LEFT OUTER JOIN SILVER.PROFISEE_DATA_ORGANIZATION ON CASE WHEN REF_PLNT_NO_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_NO_BRAND.REV_ORG_CODE WHEN REF_PLNT_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_BRAND.REV_ORG_CODE ELSE NVL(PLNT_DIM.PLANT_CODE,'NO CODE FOUND') END = PROFISEE_DATA_ORGANIZATION.ORG_CODE
GROUP BY 
EVNT_TYPE,

/*DATE FIELDS*/
DATE_UKEY ,
EVNT_DTM_SKEY,
DECODE(TRANSACTION_DAY_NAME,'Monday','MON','Tuesday','TUE','Wednesday','WED','Thursday','THU','Friday','FRI','Saturday','SAT','Sunday','SUN'),
CONCAT('FW',LPAD(CAST(DATE_DIM.FISCAL_WK_OF_YR AS INTEGER) ,2,'0')) ,
CONCAT('FY',CAST(DATE_DIM.FISCAL_YR_NUM AS INTEGER)) ,
CONCAT(CONCAT(CAST(FISCAL_MONTH_OF_YR AS INT),"/"),CAST(FISCAL_YR_NUM AS INT)) ,
FISCAL_YR_MONTH,
FISCAL_YR_NUM, 
FISCAL_YR_WK,
FISCAL_MONTH_OF_YR,
RPTD_DIM.HDR_SKEY,
RPTD_DIM.RPTD_SKEY,
/*Dimnesion Attributes */
RPTD_DIM.ORG_STRUCTURE ,
CASE WHEN REF_PLNT_NO_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_NO_BRAND.REV_ORG_CODE WHEN REF_PLNT_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_BRAND.REV_ORG_CODE ELSE NVL(PLNT_DIM.PLANT_CODE,'NO CODE FOUND') END,
PLNT_DIM.PLANT_CODE  ,
NVL(SSEG_DIM.SALES_SEGMENT,'NO SALES SEGMENT') ,
CASE WHEN RPTD_FCPL_DIM.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL_DIM.FP_SUB_CHANNEL END ,
RPTD_FCPL_DIM.FP_ORDER_TYPE,
CASE WHEN RPTD_FCPL_DIM.FP_BRAND_SERIES IN ('N/A','OTHER','UNKNOWN') THEN RPTD_FCPL_DIM.FP_BRAND_SERIES || CASE RPTD_FCPL_DIM.ORG_STRUCTURE  WHEN 'WOOD WINDOWS AND PATIO DOORS' THEN ' WOOD' WHEN 'VINYL WINDOWS AND PATIO DOORS' THEN ' VINYL' WHEN 'FIBERGLASS WINDOWS AND PATIO DOORS' THEN ' FIBERGLASS' ELSE CONCAT(' ',RPTD_FCPL_DIM.ORG_STRUCTURE) END ELSE RPTD_FCPL_DIM.FP_BRAND_SERIES END ,
PKDG_FCPL_DIM.FP_PRODUCT,
NVL(PROFISEE_DATA_PRODUCT_FAMILY.NAME,'UNKNOWN PRODUCT FAMILY') ,
PKDG_FCPL_DIM.FP_EXT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_GRILLE,
PKDG_FCPL_DIM.FP_INT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_SCREEN,
PKDG_FCPL_DIM.FP_TWO_COLOR,
CHAN_DIM.CLIENT_CODE ,


/*PROFIEE CODES*/
PROFISEE_DATA_BUSINESS_STRUCTURE.CODE ,

PROFISEE_DATA_SALES_SEGMENT.CODE ,
PROFISEE_DATA_SALES_SEGMENT.MEMBER ,  
PROFISEE_DATA_SALES_SEGMENT.MEMBER_DESCRIPTION ,  

PROFISEE_DATA_SEGMENT_BUILDING_TYPE.CODE ,
PROFISEE_DATA_SEGMENT_BUILDING_TYPE.MEMBER ,
PROFISEE_DATA_SEGMENT_BUILDING_TYPE.MEMBER_DESCRIPTION ,

PROFISEE_DATA_SEGMENT.CODE ,
PROFISEE_DATA_SEGMENT.MEMBER ,  
PROFISEE_DATA_SEGMENT.MEMBER_DESCRIPTION ,  


PROFISEE_DATA_BRAND_SERIES.CODE ,
PROFISEE_DATA_BRAND_SERIES.MEMBER  ,
PROFISEE_DATA_BRAND_SERIES.MEMBER_DESCRIPTION  ,

PROFISEE_DATA_PRODUCT.CODE    ,
PROFISEE_DATA_PRODUCT.MEMBER    ,
PROFISEE_DATA_PRODUCT.MEMBER_DESCRIPTION   ,

PROFISEE_DATA_PRODUCT_FAMILY.CODE ,
PROFISEE_DATA_PRODUCT_FAMILY.MEMBER ,
PROFISEE_DATA_PRODUCT_FAMILY.MEMBER_DESCRIPTION ,



PROFISEE_DATA_FP_SUB_CHANNEL.CODE ,
PROFISEE_DATA_FP_SUB_CHANNEL.MEMBER ,
PROFISEE_DATA_FP_SUB_CHANNEL.MEMBER_DESCRIPTION ,


PROFISEE_DATA_BRANCH_ACCOUNT_CODE.CODE ,
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.MEMBER ,
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.MEMBER_DESCRIPTION ,   

PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.CODE ,
PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.MEMBER ,
PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.MEMBER_DESCRIPTION ,    


case when REF_PLNT_NO_BRAND.mfg_Org_Code is not null   then REF_PLNT_NO_BRAND.Revenue_Organization when REF_PLNT_BRAND.mfg_org_code is not null  then REF_PLNT_BRAND.Revenue_Organization else nvl(org.Code,'NO CODE FOUND') end,


concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(
concat(PROFISEE_DATA_BUSINESS_STRUCTURE.CODE,'|'),
PROFISEE_DATA_BRAND_SERIES.CODE),'|'),
PROFISEE_DATA_PRODUCT.CODE),'|'),
case when REF_PLNT_NO_BRAND.mfg_Org_Code is not null  then REF_PLNT_NO_BRAND.Revenue_Organization when REF_PLNT_BRAND.mfg_org_code is not null then REF_PLNT_BRAND.Revenue_Organization else nvl(org.Code,'NO CODE FOUND') end ),'|'),
PROFISEE_DATA_FP_SUB_CHANNEL.CODE),'|'),
PROFISEE_DATA_BRANCH_ACCOUNT_NUMBER.CODE),'|'),
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.CODE),'|'),
PROFISEE_DATA_SEGMENT.CODE),'|'),
PROFISEE_DATA_SEGMENT_BUILDING_TYPE.CODE),'|'),
PROFISEE_DATA_SALES_SEGMENT.CODE),'|'),
FP_ORDER_TYPE)

-- COMMAND ----------


CREATE OR REPLACE VIEW SILVER.PEL_ONESTREAM_EDW_BOOKING_INVOICE_CUBE_V 
AS SELECT
SCENARIO, EVNT_TYPE,  FISCAL_WEEK, FISCAL_YEAR, SUBSTRING(EVNT_DTM,1,10) EVNT_DTM, WEEK_DAY, ORG_CODE, SALES_SEGMENT_MEMBER_CODE, 	SALES_SEGMENT,	FP_ORDER_TYPE,	BRAND_SERIES_MEMBER_CODE,FP_BRAND_SERIES,	PRODUCT_FAMILY_MEMBER_CODE,	FP_PRODUCT,	PRODUCT_FAMILY,	FP_SUB_CHANNEL_MEMBER_CODE,	FP_SUB_CHANNEL,	BRANCH_ACCOUNT_CODE_MEMBER_CODE,	BRANCH_ACCOUNT_CODE, COMPLETE_UNIT_QTY,COMPLETE_UNIT_EXT_GROSS_AMT, EXT_GROSS_AMT -COMPLETE_UNIT_EXT_GROSS_AMT NOT_COMPLETE_EXT_GROSS_AMT , PURCHASE_QTY - COMPLETE_UNIT_QTY NOT_COMPLETE_UNIT_QTY
FROM silver.PEL_ONESTREAM_EDW_AGG_FCPL_V


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_GP_BAT_TB_ACTUAL_V AS SELECT 

'Actual' Scenario,
concat('FY',YEAR1) FISCAL_YEAR,
CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
substring(ACTNUMST,1,8) Entity,
substring(ACTNUMST,5,4) Account,
ACTDESCR Description,
substring(ACTNUMST,10,3) Cost_Center,
substring(ACTNUMST,14,3) Segment,
'' ICP,
sum(PERIOD_BALANCE) Amount,
'peohmsgp01_bat' SOURCE,
TRIM(ACTNUMST) ACTNUMST
FROM 
(
select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 1
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 2
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 3
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 4
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 5
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 6
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 7
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 8
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 9
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 10
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <=11
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_bat_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_bat_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_bat_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_bat_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 12
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
) GL
GROUP BY 
year1, periodid, ACTNUMST, ACTDESCR


-- COMMAND ----------

CREATE or replace VIEW silver.pel_onestream_gp_rei_tb_actual_v
AS SELECT 
'Actual' Scenario,
concat('FY',YEAR1) FISCAL_YEAR,
CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
substring(ACTNUMST,1,8) Entity,
substring(ACTNUMST,5,4) Account,
ACTDESCR Description,
substring(ACTNUMST,10,3) Cost_Center,
substring(ACTNUMST,14,3) Segment,
'' ICP,
sum(PERIOD_BALANCE) Amount,
'peohmsgp01_bat' SOURCE,
TRIM(ACTNUMST) ACTNUMST
FROM 
(
select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 1
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 2
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 3
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 4
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 5
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 6
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 7
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 8
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 9
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 10
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <=11
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 12
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
) GL
GROUP BY 
year1, periodid, ACTNUMST, ACTDESCR

-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_GP_BIR_TB_ACTUAL_V 
AS SELECT 

'Actual' Scenario,
concat('FY',YEAR1) FISCAL_YEAR,
CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
substring(ACTNUMST,1,8) Entity,
substring(ACTNUMST,5,4) Account,
ACTDESCR Description,
substring(ACTNUMST,10,3) Cost_Center,
substring(ACTNUMST,14,3) Segment,
'' ICP,
sum(PERIOD_BALANCE) Amount,
' peohmsgp01_birm' SOURCE,
TRIM(ACTNUMST) ACTNUMST
FROM 
(
select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 1
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 2
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 3
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 4
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 5
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 6
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 7
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 8
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 9
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 10
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <=11
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
  inner join silver.peohmsgp01_birm_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.peohmsgp01_birm_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.peohmsgp01_birm_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 12
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
) GL
GROUP BY 
year1, periodid, ACTNUMST, ACTDESCR


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_GP_CWS_TB_ACTUAL_V 
AS SELECT 

'Actual' Scenario,
concat('FY',YEAR1) FISCAL_YEAR,
CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
substring(ACTNUMST,16,4) Entity,
substring(ACTNUMST,1,9) Account,
ACTDESCR Description,
substring(ACTNUMST,11,4) Cost_Center,
'' Segment,
'' ICP,
sum(PERIOD_BALANCE) Amount,
'sql01_cws_cc_cws' SOURCE,
TRIM(ACTNUMST) ACTNUMST
FROM 
(
select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 1
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 2
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 3
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 4
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 5
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 6
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 7
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 8
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 9
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 10
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <=11
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.sql01_cws_cc_cws_dbo_gl10110 g  --gl_summary_data
  inner join silver.sql01_cws_cc_cws_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.sql01_cws_cc_cws_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.sql01_cws_cc_cws_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 12
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
) GL
GROUP BY 
year1, periodid, ACTNUMST, ACTDESCR


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_GP_WCL_TB_ACTUAL_V 
AS SELECT 

'Actual' Scenario,
concat('FY',YEAR1) FISCAL_YEAR,
CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
substring(ACTNUMST,1,8) Entity,
substring(ACTNUMST,5,4) Account,
ACTDESCR Description,
substring(ACTNUMST,10,3) Cost_Center,
substring(ACTNUMST,14,3) Segment,
'' ICP,
sum(PERIOD_BALANCE) Amount,
' njnhmsgp01_pwdnj' SOURCE,
TRIM(ACTNUMST) ACTNUMST
FROM 
(
select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 1
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 2
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
UNION
select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 3
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 4
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 5
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 6
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 7
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 8
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 9
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 10
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
union
select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <=11
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 12
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
) GL
GROUP BY 
year1, periodid, ACTNUMST, ACTDESCR


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_PDSN_SUB_BAC_UNITS 
AS select 
SCENARIO,	EVNT_TYPE,	'FM'||SUBSTR(FISCAL_YR_MONTH,5,2) FISCAL_MONTH , FISCAL_YEAR,	SALES_SEGMENT_MEMBER_CODE,	SALES_SEGMENT,	BRAND_SERIES_MEMBER_CODE,	FP_BRAND_SERIES,	BRANCH_ACCOUNT_CODE_MEMBER_CODE,	BRANCH_ACCOUNT_CODE,	

SUM( COMPLETE_UNIT_EXT_GROSS_AMT +NOT_COMPLETE_EXT_GROSS_AMT) EXT_GROSS_AMT	, 
SUM(COMPLETE_UNIT_QTY) PURCHASE_QTY

from SILVER.PEL_ONESTREAM_EDW_BOOKING_INVOICE_CUBE_V  INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_DATE_DIM DATE_DIM  ON PEL_ONESTREAM_EDW_BOOKING_INVOICE_CUBE_V.EVNT_DTM = DATE_DIM.DATE_UKEY 
WHERE 
FISCAL_YEAR >= 'FY2022'
and EVNT_TYPE = 'PELLA INVOICE' 
AND BRANCH_ACCOUNT_CODE IN 
(
'20900', 
'74400', 
'00200', 
'70000', 
'09600', 
'72300', 
'12900', 
'73000', 
'18900', 
'74200', 
'08300', 
'08400', 
'79500', 
'06500', 
'71800', 
'03500', 
'71100', 
'00500', 
'70300', 
'10500', 
'72400', 
'07800', 
'07900', 
'71900', 
'72100', 
'49300', 
'79300', 
'29800', 
'29900', 
'75600', 
'76300', 
'04000', 
'04100', 
'04500', 
'71300', 
'07000', 
'83300', 
'42500', 
'78600', 
'23700', 
'75100' 

)
GROUP BY 
SCENARIO,	EVNT_TYPE,	'FM'||SUBSTR(FISCAL_YR_MONTH,5,2),	FISCAL_YEAR,	SALES_SEGMENT_MEMBER_CODE,	SALES_SEGMENT,	BRAND_SERIES_MEMBER_CODE,	FP_BRAND_SERIES,	BRANCH_ACCOUNT_CODE_MEMBER_CODE,	BRANCH_ACCOUNT_CODE


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.pel_onestream_edw_dva_child_v AS 
SELECT  FISCAL_YR_NUM ,
FISCAL_WK_OF_YR, 
CASE WHEN PCAT.END_UNIT_CATEGORY ='UNIT' THEN SUBSTR(PLNT_UKEY,5,3) ELSE PLANT_CODE END CHILD_ORG, plnt_ukey,ASSEMBLY_LINE_CODE CHILD_ASSY_LINE, PCAT.product_category,  END_UNIT_CATEGORY,LI_SKEY, CASE WHEN RPTD_FCPL.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL.FP_SUB_CHANNEL END FP_SUB_CHANNEL,FP_ORDER_TYPE, NVL(SSEG.SALES_SEGMENT,'NO SALES SEGMENT') SALES_SEGMENT, CHAN.CLIENT_CODE, FP_BRAND_SERIES, FP_PRODUCT, PURCHASE_QTY

FROM (
			select * from  silver.edwproddb_edwprod_fact_agg_pqm_po_li_snapshot_fact where PELLA_INV_DTM_SKEY >  20211115  union
			select * from silver.edwproddb_edwprod_fact_agg_ora_po_li_snapshot_fact where PELLA_INV_DTM_SKEY >  20211115 
	 ) F
	INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_PCAT_DIM PCAT ON F.PCAT_SKEY = PCAT.PCAT_SKEY AND PCAT.MFG_VIEW_FLG = 1
	INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_PLNT_DIM PLNT ON F.PLNT_SKEY = PLNT.PLNT_SKEY
	INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_DATE_DIM ON F.PELLA_INV_DTM_SKEY = DATE_SKEY 
	INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_RPTD_DIM RPTD ON F.RPTD_SKEY = RPTD.RPTD_SKEY
  INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_RPTD_FCPL_DIM RPTD_FCPL ON RPTD.RPTD_FCPL_SKEY = RPTD_FCPL.RPTD_FCPL_SKEY
  INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_PKDG_FCPL_DIM PK ON F.PKDG_FCPL_SKEY = PK.PKDG_FCPL_SKEY
  LEFT OUTER JOIN SILVER.EDWPRODDB_EDWPROD_EDW_EXT_PROFISEE_DOMAIN_SSEG_V SSEG ON F.SSEG_SKEY = SSEG.SSEG_SKEY
  INNER  JOIN SILVER.EDWPRODDB_EDWPROD_EDW_EXT_PROFISEE_DOMAIN_CHAN_V  CHAN ON F.CHAN_SKEY = CHAN.CHAN_SKEY
 	

WHERE (
      (PCAT.END_UNIT_CATEGORY = 'COMPONENT UNIT')  OR
      (PCAT.PRODUCT_CATEGORY = 'UNIT' AND SUBSTR(PLNT_UKEY,4,1)= '*' )  
      
      )


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.pel_onestream_edw_dva_parent_v 
AS SELECT CASE WHEN PCAT.PRODUCT_CATEGORY ='UNIT' THEN SUBSTR(PLNT_UKEY,1,3) ELSE PLANT_CODE END PARENT_ORG, plnt_ukey, ASSEMBLY_LINE_CODE PARENT_ASSY_LINE,  product_category,  LI_SKEY , FISCAL_YR_MONTH
FROM (
			select * from  silver.edwproddb_edwprod_fact_agg_pqm_po_li_snapshot_fact where PELLA_INV_DTM_SKEY >  20211115  union
			select * from silver.edwproddb_edwprod_fact_agg_ora_po_li_snapshot_fact where PELLA_INV_DTM_SKEY >  20211115 
	 ) F
	INNER JOIN  silver.edwproddb_edwprod_dim_PCAT_DIM PCAT ON F.PCAT_SKEY = PCAT.PCAT_SKEY AND PCAT.PELLA_OPENING_VIEW_FLG = 1
	INNER JOIN  silver.edwproddb_edwprod_dim_PLNT_DIM PLNT ON F.PLNT_SKEY = PLNT.PLNT_SKEY
	INNER JOIN  silver.edwproddb_edwprod_dim_DATE_DIM ON F.PELLA_INV_DTM_SKEY = DATE_SKEY 

WHERE
(       (PCAT.PRODUCT_CATEGORY = 'COMPOSITE')  OR
        (PCAT.PRODUCT_CATEGORY = 'UNIT' AND SUBSTR(PLNT_UKEY,4,1)= '*')  --Need to get single dva units where dva plnt different than mfg plnt
)


-- COMMAND ----------


CREATE OR REPLACE VIEW SILVER.PEL_ONESTREAM_PROF_EDW_DVA_UNITS_ACTUAL_V 
AS SELECT
'Actual' SCENARIO,
'PELLA INVOICE' EVNT_TYPE,
'FW' || LPAD(cast(FISCAL_WK_OF_YR as integer),2,'0') FISCAL_WEEK,
'FY' || CAST(FISCAL_YR_NUM AS INTEGER)  FISCAL_YEAR,
PARENT_ORG,
CHILD_ORG,
PARENT_ASSY_LINE,
CHILD_ASSY_LINE,
FP_ORDER_TYPE,
profisee_data_FP_Sub_Channel.MEMBER FP_SUB_CHANNEL_MEMBER_CODE,
FP_SUB_CHANNEL,
profisee_data_Branch_Account_Code.MEMBER BRANCH_ACCT_CODE_MEMBER_CODE,
CLIENT_CODE BRANCH_ACCOUNT_CODE,
profisee_data_Sales_Segment.MEMBER SALES_SEGMENT_MEMBER_CODE,
SALES_SEGMENT,
profisee_data_Brand_Series.MEMBER BRAND_SERIES_MEMBER_CODE,
FP_BRAND_SERIES CHILD_FP_BRAND_SERIES,
profisee_data_Product_Family.MEMBER PRODUCT_FAMILY_MEMBER_CODE,
FP_PRODUCT	CHILD_FP_PRODUCT,
profisee_data_Product_Family.NAME CHILD_PRODUCT_FAMILY,
CAST(SUM(PURCHASE_QTY) AS INTEGER) PURCHASE_QTY,
CAST(FISCAL_YR_MONTH AS INTEGER) FISCAL_YR_MONTH


FROM 
SILVER.PEL_ONESTREAM_EDW_DVA_PARENT_V PARENT INNER JOIN SILVER.PEL_ONESTREAM_EDW_DVA_CHILD_V CHILD ON PARENT.LI_SKEY = CHILD.LI_SKEY
INNER  JOIN silver.profisee_data_FP_Sub_Channel on FP_SUB_CHANNEL = profisee_data_FP_Sub_Channel.SOURCE_ID  --INNER JOIN SO WE EXCLUDE NON DISTRIBUTOR ORDERS
LEFT OUTER JOIN silver.profisee_data_Branch_Account_Code  ON CLIENT_CODE = profisee_data_Branch_Account_Code.source_id
LEFT OUTER JOIN silver.profisee_data_Brand_Series  ON FP_BRAND_SERIES   = profisee_data_Brand_Series.source_id
LEFT OUTER JOIN silver.profisee_data_Product  ON FP_PRODUCT = profisee_data_Product.source_id
LEFT OUTER JOIN silver.profisee_data_Product_Family on profisee_data_Product.Product_Family = profisee_data_Product_Family.Code
LEFT OUTER JOIN silver.profisee_data_Sales_Segment  ON case when FP_SUB_CHANNEL IN ('LOWES' ,'BIG BOX','PRO DEALER') THEN 'NO SALES SEGMENT DATA' else  nvl(SALES_SEGMENT,'NO SALES SEGMENT DATA') end = profisee_data_Sales_Segment.source_id

WHERE 
(PARENT_ORG <> CHILD_ORG) 
OR 
(
  (PARENT_ORG = CHILD_ORG)  AND (PARENT_ASSY_LINE <> CHILD_ASSY_LINE)
)

GROUP BY 
'FW' || LPAD(cast(FISCAL_WK_OF_YR as integer),2,'0') ,
 'FY' || CAST(FISCAL_YR_NUM AS INTEGER) ,
PARENT_ORG,
CHILD_ORG,
PARENT_ASSY_LINE,
CHILD_ASSY_LINE,
FP_ORDER_TYPE,
profisee_data_FP_Sub_Channel.MEMBER ,
FP_SUB_CHANNEL,
profisee_data_Branch_Account_Code.MEMBER ,
CLIENT_CODE ,
profisee_data_Sales_Segment.MEMBER ,
SALES_SEGMENT,
profisee_data_Brand_Series.MEMBER ,
FP_BRAND_SERIES ,
profisee_data_Product_Family.MEMBER ,
FP_PRODUCT	,
profisee_data_Product_Family.NAME ,
PURCHASE_QTY,
CAST(FISCAL_YR_MONTH AS INTEGER)


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_PROF_EDW_GL_TIEOUT_ACTUAL_V 
AS select 
'Actual' SCENARIO,
'PELLA INVOICE' EVNT_TYPE,
CONCAT('FW',LPAD(CAST(DATE_DIM.FISCAL_WK_OF_YR AS INTEGER) ,2,'0')) FISCAL_WEEK,
CONCAT('FY',CAST(DATE_DIM.FISCAL_YR_NUM AS INTEGER)) FISCAL_YEAR,
gl_tieout_amt.org_code,

'SS1001' SALES_SEGMENT_MEMBER_CODE,  --hard coded for gltieout
'TRADE REMODEL' SALES_SEGMENT,  --hard coded for gltieout

'SOS' FP_ORDER_TYPE, --hard coded for gltieout
CASE			
	WHEN gl_tieout_amt.ORG_CODE ='M03'	THEN 'BS1044'
	WHEN gl_tieout_amt.ORG_CODE = 'P02' 	THEN 'BS1038'
	WHEN gl_tieout_amt.ORG_CODE = 'G03' 	THEN 'BS1041'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'C10' 	THEN 'BS1038'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'S12' 	THEN 'BS1038'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P07' 	THEN 'BS1038'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P06' 	THEN 'BS1038'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P08' 	THEN 'BS1038'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'M02' 	THEN 'BS1041'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'V02' 	THEN 'BS1041'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'V04' 	THEN 'BS1041'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P52' 	THEN 'BS1041'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'S05' 	THEN 'BS1038'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'M06' 	THEN 'BS1041'
ELSE 'BS1038' END BRAND_SERIES_MEMBER_CODE,

CASE			
	WHEN GL_TIEOUT_AMT.ORG_CODE ='M03'	THEN 'N/A FIBERGLASS'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P02' 	THEN 'N/A WOOD'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'G03' 	THEN 'N/A VINYL'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'C10' 	THEN 'N/A WOOD'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'S12' 	THEN 'N/A WOOD'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P07' 	THEN 'N/A WOOD'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P06' 	THEN 'N/A WOOD'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P08' 	THEN 'N/A WOOD'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'M02' 	THEN 'N/A VINYL'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'V02' 	THEN 'N/A VINYL'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'V04' 	THEN 'N/A VINYL'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'P52' 	THEN 'N/A VINYL'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'S05' 	THEN 'N/A WOOD'
	WHEN GL_TIEOUT_AMT.ORG_CODE = 'M06' 	THEN 'N/A VINYL'
ELSE 'N/A WOOD' END FP_BRAND_SERIES,

'PF1017'  PRODUCT_FAMILY_MEMBER_CODE,  --hard code to no product family
'GL TIEOUT' FP_PRODUCT,
'NO PRODUCT FAMILY' PRODUCT_FAMILY,

'' FP_EXT_PANEL_FNSH,
'' FP_GRILLE,
'' FP_INT_PANEL_FNSH,
'' FP_SCREEN,
'' FP_TWO_COLOR,

'FPSC1004' FP_SUB_CHANNEL_MEMBER_CODE,
'PDSN' FP_SUB_CHANNEL,
'' BRANCH_ACCOUNT_CODE_MEMBER_CODE,
'' BRANCH_ACCOUNT_CODE,
case fiscal_month_of_yr when 1 then 'DEC' WHEN 2 THEN 'JAN' WHEN 3 THEN 'FEB' WHEN 4 THEN 'MAR' WHEN 5 THEN 'APR' WHEN 6 THEN 'MAY' WHEN 7 THEN 'JUN' WHEN 8 THEN 'JUL' WHEN 9 THEN 'AUG' WHEN 10 THEN 'SEP' WHEN 11 THEN 'OCT' WHEN 12 THEN 'NOV' END MONTH_NAME,
gl_plnt_amt, 
GL_PLNT_AMT - edw_gross_sales.ext_gross_amt EXT_GROSS_AMT,
gl_total_amt,


date_dim.fiscal_yr_month,
edw_gross_sales.ext_gross_amt tot_gross_amt

from 

(
select evnt_dtm_skey, gl_total_amt, 'P07' ORG_CODE,	ACCESSORY_PLANT gl_plnt_amt	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt,'P06' ORG_CODE, 	DOOR_PLANT	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'M03' ORG_CODE,	MURRAY_FIBERGLASS	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'M06' ORG_CODE,	MACOMB	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'P52' ORG_CODE,	PORTLAND	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'P02',	SERVICE_PARTS_PLANT	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'S05',	SHENANDOAH	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'C10',	CARROLL	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'S12',	SIOUX_CENTER	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'G03',	GETTYSBURG	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'P08',	WINDOWS_PLANT	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'M02',	MURRAY_VINYL	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	union
select evnt_dtm_skey, gl_total_amt, 'V02',	REIDSVILLE	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	UNION
select evnt_dtm_skey, gl_total_amt, 'V04',	TROY	from silver.edwproddb_edwprod_edw_ext_finance_gl_tieout_stg_v WHERE EVNT_DTM_SKEY > 20221201	
) gl_tieout_amt  INNER JOIN silver.edwproddb_edwprod_dim_date_dim date_dim on gl_tieout_amt.evnt_DTM_skey = date_dim.date_skey
inner join (select org_code, fiscal_yr_month, sum(ext_gross_amt) ext_gross_amt from silver.pel_onestream_prof_edw_total_grossed_up_actual_v group by org_code, fiscal_yr_month)  edw_gross_sales on edw_gross_sales.FISCAL_YR_MONTH =     date_dim.fiscal_yr_month  and gl_tieout_amt.org_code = edw_gross_sales.org_code


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_PROF_EDW_PRICE_ACTUAL_V AS SELECT
'ACTUAL' SCENARIO,
EVNT_TYPE,

/*DATE FIELDS*/

CONCAT('FW',LPAD(CAST(DATE_DIM.FISCAL_WK_OF_YR AS INTEGER) ,2,'0')) FISCAL_WEEK,
CONCAT('FY',CAST(DATE_DIM.FISCAL_YR_NUM AS INTEGER)) FISCAL_YEAR,
CASE WHEN REF_PLNT_NO_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_NO_BRAND.REV_ORG_CODE WHEN REF_PLNT_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_BRAND.REV_ORG_CODE ELSE NVL(PLNT_DIM.PLANT_CODE,'NO CODE FOUND') END ORG_CODE,

PROFISEE_DATA_SALES_SEGMENT.CODE SALES_SEGMENT_MEMBER_CODE,
NVL(SSEG_DIM.SALES_SEGMENT,'NO SALES SEGMENT') SALES_SEGMENT,
RPTD_FCPL_DIM.FP_ORDER_TYPE,
PROFISEE_DATA_BRAND_SERIES.MEMBER  BRAND_SERIES_MEMBER_CODE,
CASE WHEN RPTD_FCPL_DIM.FP_BRAND_SERIES IN ('N/A','OTHER','UNKNOWN') THEN RPTD_FCPL_DIM.FP_BRAND_SERIES || CASE RPTD_FCPL_DIM.ORG_STRUCTURE  WHEN 'WOOD WINDOWS AND PATIO DOORS' THEN ' WOOD' WHEN 'VINYL WINDOWS AND PATIO DOORS' THEN ' VINYL' WHEN 'FIBERGLASS WINDOWS AND PATIO DOORS' THEN ' FIBERGLASS' ELSE CONCAT(' ',RPTD_FCPL_DIM.ORG_STRUCTURE) END ELSE RPTD_FCPL_DIM.FP_BRAND_SERIES END FP_BRAND_SERIES,
PROFISEE_DATA_PRODUCT_FAMILY.MEMBER PRODUCT_FAMILY_MEMBER_CODE,
PKDG_FCPL_DIM.FP_PRODUCT,   
NVL(PROFISEE_DATA_PRODUCT_FAMILY.NAME,'UNKNOWN PRODUCT FAMILY') PRODUCT_FAMILY,

PKDG_FCPL_DIM.FP_EXT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_GRILLE,
PKDG_FCPL_DIM.FP_INT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_SCREEN,
PKDG_FCPL_DIM.FP_TWO_COLOR,
PROFISEE_DATA_FP_SUB_CHANNEL.MEMBER FP_SUB_CHANNEL_MEMBER_CODE,
CASE WHEN RPTD_FCPL_DIM.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL_DIM.FP_SUB_CHANNEL END FP_SUB_CHANNEL,
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.CODE BRANCH_ACCOUNT_CODE_MEMBER_CODE,
CHAN_DIM.CLIENT_CODE BRANCH_ACCOUNT_CODE,
/*Measures */
0 gross_amt,
	0 gross_base_amt,
	0 purchase_qty,
	0 grille_qty,
	0 screen_qty,
	0 uacc_qty,
	0 door_panel_qty,
	0 matl_cost_amt,
	sum(ext_price_amt) price_amt

FROM 
silver.edwproddb_edwprod_edw_ext_dl_agg_price_po_li_da_fact_v  FACT 
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_EVNT_DIM EVNT_DIM  ON FACT.EVNT_SKEY = EVNT_DIM.EVNT_SKEY 
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_DATE_DIM DATE_DIM  ON FACT.EVNT_DTM_SKEY = DATE_DIM.DATE_SKEY 
   INNER JOIN  SILVER.EDWPRODDB_EDWPROD_DIM_RPTD_DIM RPTD_DIM ON FACT.RPTD_SKEY = RPTD_DIM.RPTD_SKEY
   LEFT OUTER JOIN SILVER.EDWPRODDB_EDWPROD_EDW_EXT_PROFISEE_DOMAIN_SSEG_V  SSEG_DIM ON RPTD_DIM.SSEG_SKEY = SSEG_DIM.SSEG_SKEY                                  
   LEFT OUTER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_CHAN_DIM CHAN_DIM ON RPTD_DIM.CHAN_SKEY = CHAN_DIM.CHAN_SKEY  
   
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_RPTD_FCPL_DIM RPTD_FCPL_DIM ON RPTD_DIM.RPTD_FCPL_SKEY = RPTD_FCPL_DIM.RPTD_FCPL_SKEY  
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_PLNT_DIM  PLNT_DIM ON RPTD_DIM.PLNT_SKEY = PLNT_DIM.PLNT_SKEY AND PLNT_DIM.PLANT_CODE NOT IN ('COL')   
   INNER JOIN SILVER.EDWPRODDB_EDWPROD_DIM_PKDG_FCPL_DIM  PKDG_FCPL_DIM ON FACT.PKDG_FCPL_SKEY = PKDG_FCPL_DIM.PKDG_FCPL_SKEY  AND PKDG_FCPL_DIM.FP_PRODUCT NOT IN ( 'STORM DOOR')
   
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_SALES_SEGMENT  ON case when RPTD_FCPL_DIM.FP_SUB_CHANNEL IN ( 'LOWES', 'BIG BOX','PRO DEALER') THEN 'NO SALES SEGMENT DATA' else  NVL(SSEG_DIM.SALES_SEGMENT,'NO SALES SEGMENT DATA') END = PROFISEE_DATA_SALES_SEGMENT.NAME
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_SEGMENT_BUILDING_TYPE   ON PROFISEE_DATA_SALES_SEGMENT.SEGMENT_BUILDING_TYPE =  PROFISEE_DATA_SEGMENT_BUILDING_TYPE.CODE
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_SEGMENT  ON PROFISEE_DATA_SEGMENT_BUILDING_TYPE.SEGMENT = PROFISEE_DATA_SEGMENT.CODE
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_BRANCH_ACCOUNT_CODE  ON CHAN_DIM.CLIENT_CODE = PROFISEE_DATA_BRANCH_ACCOUNT_CODE.SOURCE_ID
   LEFT OUTER JOIN  SILVER.PROFISEE_DATA_FP_SUB_CHANNEL  ON CASE WHEN RPTD_FCPL_DIM.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL_DIM.FP_SUB_CHANNEL END  = PROFISEE_DATA_FP_SUB_CHANNEL.NAME  
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_BUSINESS_STRUCTURE  ON RPTD_DIM.ORG_STRUCTURE = PROFISEE_DATA_BUSINESS_STRUCTURE.NAME
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_BRAND_SERIES  ON CASE WHEN RPTD_FCPL_DIM.FP_BRAND_SERIES IN ('N/A','OTHER','UNKNOWN') THEN RPTD_FCPL_DIM.FP_BRAND_SERIES || CASE RPTD_FCPL_DIM.ORG_STRUCTURE  WHEN 'WOOD WINDOWS AND PATIO DOORS' THEN ' WOOD' WHEN 'VINYL WINDOWS AND PATIO DOORS' THEN ' VINYL' WHEN 'FIBERGLASS WINDOWS AND PATIO DOORS' THEN ' FIBERGLASS' ELSE CONCAT(' ',RPTD_FCPL_DIM.ORG_STRUCTURE) END ELSE RPTD_FCPL_DIM.FP_BRAND_SERIES END   = PROFISEE_DATA_BRAND_SERIES.NAME
   LEFT OUTER JOIN  SILVER.PROFISEE_DATA_PRODUCT ON PKDG_FCPL_DIM.FP_PRODUCT = PROFISEE_DATA_PRODUCT.SOURCE_ID  
   LEFT OUTER JOIN  SILVER.PROFISEE_DATA_PRODUCT_FAMILY ON PROFISEE_DATA_PRODUCT.PRODUCT_FAMILY = PROFISEE_DATA_PRODUCT_FAMILY.CODE
   
  
   
   LEFT OUTER JOIN SILVER.PROFISEE_DATA_REVENUE_PLANT_MOVES REF_PLNT_NO_BRAND ON CASE
                                                               WHEN PLNT_DIM.PLANT_CODE = 'N/A' THEN 'S06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SPDEPOT' THEN 'P02'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SHENANDO' THEN 'S06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'PELLCASE' THEN 'P05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'ACPLANT' THEN 'P07'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SIOUXCTR' THEN 'S12'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SUPPORT' THEN 'P08'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'DOOR' THEN 'P06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'METAL' THEN 'P10'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'CARROLL' THEN 'C10'
                                                            ELSE PLNT_DIM.PLANT_CODE
                                                            END = REF_PLNT_NO_BRAND.MFG_ORG_CODE AND REF_PLNT_NO_BRAND.`MANUFACTURING_BRAND_SERIES.NAME` IS  NULL

   LEFT OUTER JOIN SILVER.PROFISEE_DATA_REVENUE_PLANT_MOVES  REF_PLNT_BRAND ON CASE
                                                               WHEN PLNT_DIM.PLANT_CODE = 'N/A' THEN 'S06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SPDEPOT' THEN 'P02'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SHENANDO' THEN 'S06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'PELLCASE' THEN 'P05'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'ACPLANT' THEN 'P07'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SIOUXCTR' THEN 'S12'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'SUPPORT' THEN 'P08'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'DOOR' THEN 'P06'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'METAL' THEN 'P10'
                                                               WHEN PLNT_DIM.PLANT_CODE = 'CARROLL' THEN 'C10'
                                                            ELSE PLNT_DIM.PLANT_CODE
                                                            END = REF_PLNT_BRAND.MFG_ORG_CODE AND RPTD_FCPL_DIM.FP_BRAND_SERIES = REF_PLNT_BRAND.`MANUFACTURING_BRAND_SERIES.NAME`
                                                            
  LEFT OUTER JOIN SILVER.PROFISEE_DATA_ORGANIZATION ON CASE WHEN REF_PLNT_NO_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_NO_BRAND.REV_ORG_CODE WHEN REF_PLNT_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_BRAND.REV_ORG_CODE ELSE DECODE(PLNT_DIM.PLANT_CODE,'N/A','S05',NVL(PLNT_DIM.PLANT_CODE,'NO CODE FOUND')) END = PROFISEE_DATA_ORGANIZATION.ORG_CODE
  
  
GROUP BY 
EVNT_TYPE,

/*DATE FIELDS*/
DATE_UKEY ,
EVNT_DTM_SKEY,
DECODE(TRANSACTION_DAY_NAME,'Monday','MON','Tuesday','TUE','Wednesday','WED','Thursday','THU','Friday','FRI','Saturday','SAT','Sunday','SUN'),
CONCAT('FW',LPAD(CAST(DATE_DIM.FISCAL_WK_OF_YR AS INTEGER) ,2,'0')) ,
CONCAT('FY',CAST(DATE_DIM.FISCAL_YR_NUM AS INTEGER)) ,
CONCAT(CONCAT(CAST(FISCAL_MONTH_OF_YR AS INT),"/"),CAST(FISCAL_YR_NUM AS INT)) ,
FISCAL_YR_MONTH,
FISCAL_YR_NUM, 
FISCAL_YR_WK,
FISCAL_MONTH_OF_YR,

/*Dimnesion Attributes */
RPTD_DIM.ORG_STRUCTURE ,
CASE WHEN REF_PLNT_NO_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_NO_BRAND.REV_ORG_CODE WHEN REF_PLNT_BRAND.MFG_ORG_CODE IS NOT NULL  THEN REF_PLNT_BRAND.REV_ORG_CODE ELSE NVL(PLNT_DIM.PLANT_CODE,'NO CODE FOUND') END ,
PLNT_DIM.PLANT_CODE  ,
NVL(SSEG_DIM.SALES_SEGMENT,'NO SALES SEGMENT') ,
CASE WHEN RPTD_FCPL_DIM.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL_DIM.FP_SUB_CHANNEL END ,
RPTD_FCPL_DIM.FP_ORDER_TYPE,
CASE WHEN RPTD_FCPL_DIM.FP_BRAND_SERIES IN ('N/A','OTHER','UNKNOWN') THEN RPTD_FCPL_DIM.FP_BRAND_SERIES || CASE RPTD_FCPL_DIM.ORG_STRUCTURE  WHEN 'WOOD WINDOWS AND PATIO DOORS' THEN ' WOOD' WHEN 'VINYL WINDOWS AND PATIO DOORS' THEN ' VINYL' WHEN 'FIBERGLASS WINDOWS AND PATIO DOORS' THEN ' FIBERGLASS' ELSE CONCAT(' ',RPTD_FCPL_DIM.ORG_STRUCTURE) END ELSE RPTD_FCPL_DIM.FP_BRAND_SERIES END ,
PKDG_FCPL_DIM.FP_PRODUCT,
NVL(PROFISEE_DATA_PRODUCT_FAMILY.NAME,'UNKNOWN PRODUCT FAMILY') ,
PKDG_FCPL_DIM.FP_EXT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_GRILLE,
PKDG_FCPL_DIM.FP_INT_PANEL_FNSH,
PKDG_FCPL_DIM.FP_SCREEN,
PKDG_FCPL_DIM.FP_TWO_COLOR,
CHAN_DIM.CLIENT_CODE ,


/*PROFIEE CODES*/
PROFISEE_DATA_BUSINESS_STRUCTURE.CODE ,

PROFISEE_DATA_SALES_SEGMENT.CODE ,
PROFISEE_DATA_SALES_SEGMENT.MEMBER ,  
PROFISEE_DATA_SALES_SEGMENT.MEMBER_DESCRIPTION ,  

PROFISEE_DATA_SEGMENT.CODE ,
PROFISEE_DATA_SEGMENT.MEMBER ,  
PROFISEE_DATA_SEGMENT.MEMBER_DESCRIPTION ,  


PROFISEE_DATA_BRAND_SERIES.CODE ,
PROFISEE_DATA_BRAND_SERIES.MEMBER  ,
PROFISEE_DATA_BRAND_SERIES.MEMBER_DESCRIPTION  ,

PROFISEE_DATA_PRODUCT.CODE    ,
PROFISEE_DATA_PRODUCT.MEMBER    ,
PROFISEE_DATA_PRODUCT.MEMBER_DESCRIPTION   ,

PROFISEE_DATA_PRODUCT_FAMILY.CODE ,
PROFISEE_DATA_PRODUCT_FAMILY.MEMBER ,
PROFISEE_DATA_PRODUCT_FAMILY.MEMBER_DESCRIPTION ,



PROFISEE_DATA_FP_SUB_CHANNEL.CODE ,
PROFISEE_DATA_FP_SUB_CHANNEL.MEMBER ,
PROFISEE_DATA_FP_SUB_CHANNEL.MEMBER_DESCRIPTION ,


PROFISEE_DATA_BRANCH_ACCOUNT_CODE.CODE ,
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.MEMBER ,
PROFISEE_DATA_BRANCH_ACCOUNT_CODE.MEMBER_DESCRIPTION ,    

PROFISEE_DATA_ORGANIZATION.CODE,
PROFISEE_DATA_ORGANIZATION.ORG_CODE


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.pel_onestream_prof_edw_promo_gross_up_actual_v 
AS select
hdr_dim.ORACLE_order_num ,
PCT.promo_pct,

SCENARIO           ,
EVNT_TYPE         ,
FISCAL_WEEK    ,
FISCAL_YEAR     ,
ORG_CODE            ,
SALES_SEGMENT_MEMBER_CODE           ,
SALES_SEGMENT             ,
FP_ORDER_TYPE              ,
BRAND_SERIES_MEMBER_CODE               ,
FP_BRAND_SERIES          ,
PRODUCT_FAMILY_MEMBER_CODE        ,
FP_PRODUCT    ,
PRODUCT_FAMILY          ,
FP_EXT_PANEL_FNSH    ,
FP_GRILLE           ,
FP_INT_PANEL_FNSH     ,
FP_SCREEN         ,
FP_TWO_COLOR              ,
FP_SUB_CHANNEL_MEMBER_CODE        ,
FP_SUB_CHANNEL          ,
BRANCH_ACCOUNT_CODE_MEMBER_CODE        ,
BRANCH_ACCOUNT_CODE          ,
SUM((EXT_GROSS_AMT / ((100-PCT.PROMO_PCT)/100))   - ext_gross_amt )  AS EXT_GROSS_AMT  ,
SUM(EXT_GROSS_AMT )  AS EXT_DISCOUNTED_AMT  ,

0  PURCHASE_QTY ,
0  DOOR_PANEL_QTY ,
0  MATERIAL_COST_AMT ,
0 COMPLETE_UNIT_QTY ,
0 EXT_SELLING_PRICE_AMT          ,
0 PRICE_AMT  ,
FISCAL_YR_MONTH,
FM_FY,
 BUSINESS_STRUCTURE_CODE,
 BRAND_SERIES_CODE,
 FP_PRODUCT_CODE,
 FP_SUB_CHANNEL_CODE,
 SEGMENT_CODE,
 
SALES_SEGMENT_CODE,
ORGANIZATION_CODE,
DL_INS_DTM

FROM silver.pel_onestream_edw_agg_fcpl_v
 INNER JOIN ( SELECT HDR_SKEY, MIN(ORACLE_ORDER_NUM) ORACLE_ORDER_NUM FROM  silver.edwproddb_edwprod_edw_ext_dl_po_li_dim_v WHERE CUR_FLG = 1 GROUP BY HDR_SKEY ) HDR_DIM ON pel_onestream_edw_agg_fcpl_v.HDR_SKEY = HDR_DIM.HDR_SKEY 
   inner join   gold.EBS_LOWES_PROMO_ORDERS_V PCT  on hdr_dim.ORACLE_ORDER_NUM = PCT.ebs_order_number


WHERE 
ORIG_PLANT_CODE NOT IN ('CW1','COL') AND
FP_PRODUCT NOT IN ( 'STORM DOOR') and
EVNT_TYPE = 'PELLA INVOICE' AND
FP_SUB_CHANNEL = 'BIG BOX'  AND -- THIS IS ONLY LOWES 
FISCAL_YEAR = 'FY2023' 
GROUP BY
hdr_dim.ORACLE_order_num ,
PCT.promo_pct,
SCENARIO           ,
EVNT_TYPE         ,
FISCAL_WEEK    ,
FISCAL_YEAR     ,
FISCAL_YR_MONTH,
FM_FY,
ORG_CODE            ,
SALES_SEGMENT_MEMBER_CODE           ,
SALES_SEGMENT             ,
FP_ORDER_TYPE              ,
BRAND_SERIES_MEMBER_CODE               ,
FP_BRAND_SERIES          ,
PRODUCT_FAMILY_MEMBER_CODE        ,
FP_PRODUCT    ,
PRODUCT_FAMILY          ,
FP_EXT_PANEL_FNSH    ,
FP_GRILLE           ,
FP_INT_PANEL_FNSH     ,
FP_SCREEN         ,
FP_TWO_COLOR              ,
FP_SUB_CHANNEL_MEMBER_CODE        ,
FP_SUB_CHANNEL          ,
BRANCH_ACCOUNT_CODE_MEMBER_CODE        ,
BRANCH_ACCOUNT_CODE  ,
 BUSINESS_STRUCTURE_CODE,
 BRAND_SERIES_CODE,
 FP_PRODUCT_CODE,
 FP_SUB_CHANNEL_CODE,
 SEGMENT_CODE,
 
SALES_SEGMENT_CODE,
ORGANIZATION_CODE,
DL_INS_DTM


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_PROF_EDW_SALES_ACTUAL_V 
AS SELECT 
SCENARIO           ,
EVNT_TYPE         ,
FISCAL_WEEK    ,
FISCAL_YEAR     ,
ORG_CODE            ,
SALES_SEGMENT_MEMBER_CODE           ,
SALES_SEGMENT             ,
FP_ORDER_TYPE              ,
BRAND_SERIES_MEMBER_CODE               ,
FP_BRAND_SERIES          ,
PRODUCT_FAMILY_MEMBER_CODE        ,
FP_PRODUCT    ,
PRODUCT_FAMILY          ,
FP_EXT_PANEL_FNSH    ,
FP_GRILLE           ,
FP_INT_PANEL_FNSH     ,
FP_SCREEN         ,
FP_TWO_COLOR              ,
FP_SUB_CHANNEL_MEMBER_CODE        ,
FP_SUB_CHANNEL          ,
BRANCH_ACCOUNT_CODE_MEMBER_CODE        ,
BRANCH_ACCOUNT_CODE          ,
SUM(EXT_GROSS_AMT)  AS EXT_GROSS_AMT          ,
CAST(SUM(PURCHASE_QTY) AS INTEGER) AS PURCHASE_QTY ,
CAST(SUM(DOOR_PANEL_QTY) AS INTEGER) DOOR_PANEL_QTY ,
SUM(MATERIAL_COST_AMT) AS MATERIAL_COST_AMT ,
CAST(SUM(COMPLETE_UNIT_QTY) AS INTEGER) COMPLETE_UNIT_QTY ,
SUM(EXT_SELLING_PRICE_AMT)  AS EXT_SELLING_PRICE_AMT          ,
SUM(PRICE_AMT)      AS PRICE_AMT  ,
FISCAL_YR_MONTH,
FM_FY,
 BUSINESS_STRUCTURE_CODE,
 BRAND_SERIES_CODE,
 FP_PRODUCT_CODE,
 FP_SUB_CHANNEL_CODE,
 SEGMENT_CODE,
 
SALES_SEGMENT_CODE,
ORGANIZATION_CODE,
DL_INS_DTM

FROM SILVER.PEL_ONESTREAM_EDW_AGG_FCPL_V  
WHERE 
EVNT_TYPE = 'PELLA INVOICE' AND
ORIG_PLANT_CODE NOT IN ('CW1','COL') AND
FP_PRODUCT NOT IN ( 'STORM DOOR')
GROUP BY
SCENARIO           ,
EVNT_TYPE         ,
FISCAL_WEEK    ,
FISCAL_YEAR     ,
FISCAL_YR_MONTH,
FM_FY,
ORG_CODE            ,
SALES_SEGMENT_MEMBER_CODE           ,
SALES_SEGMENT             ,
FP_ORDER_TYPE              ,
BRAND_SERIES_MEMBER_CODE               ,
FP_BRAND_SERIES          ,
PRODUCT_FAMILY_MEMBER_CODE        ,
FP_PRODUCT    ,
PRODUCT_FAMILY          ,
FP_EXT_PANEL_FNSH    ,
FP_GRILLE           ,
FP_INT_PANEL_FNSH     ,
FP_SCREEN         ,
FP_TWO_COLOR              ,
FP_SUB_CHANNEL_MEMBER_CODE        ,
FP_SUB_CHANNEL          ,
BRANCH_ACCOUNT_CODE_MEMBER_CODE        ,
BRANCH_ACCOUNT_CODE  ,
 BUSINESS_STRUCTURE_CODE,
 BRAND_SERIES_CODE,
 FP_PRODUCT_CODE,
 FP_SUB_CHANNEL_CODE,
 SEGMENT_CODE,
 
SALES_SEGMENT_CODE,
ORGANIZATION_CODE,
DL_INS_DTM


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.pel_onestream_prof_wms_xdock_po_li_skey_v 
AS WITH SHIP_TBL as     --Get LI_SKEY for only complete units
(
	SELECT LI_SKEY, (CASE WHEN PO_LI_SHTL_ACT_DTM IS NOT NULL THEN 1 ELSE 0 END) SHUTTLE_FLG, ROUTE_SHIP_FROM_CITY_DESC SHIP_FROM_CITY FROM
	 silver.edwproddb_edwprod_fact_agg_ship_da_fact S_f --ON F.LI_SKEY = S_F.LI_SKEY
	inner join  silver.edwproddb_edwprod_DIM_rtst_dim rtst on S_f.RTST_MSTR_SKEY = rtst.rtst_skey
	INNER JOIN  silver.edwproddb_edwprod_DIM_RPTD_DIM RPTD ON S_F.RPRT_SKEY = RPTD.RPTD_SKEY
	INNER JOIN  silver.edwproddb_edwprod_DIM_PCAT_DIM S_PCAT ON RPTD.PCAT_SKEY = S_PCAT.PCAT_SKEY AND S_PCAT.PRODUCT_CATEGORY IN ( 'COMPOSITE' ,'UNIT')
    WHERE ACT_SHIP_DTM_SKEY > 20211115 
)

SELECT DISTINCT F.PO_LI_SKEY, SHUTTLE_FLG,  SHIP_FROM_CITY
FROM

 (  select * from  silver.edwproddb_edwprod_fact_agg_pqm_po_li_snapshot_fact where  PELLA_INV_DTM_SKEY >  20211115 
		union
		select * from  silver.edwproddb_edwprod_fact_agg_ORA_po_li_snapshot_fact where  PELLA_INV_DTM_SKEY >  20211115 
	) f
	inner join  silver.edwproddb_edwprod_DIM_pcat_dim pcat on F.pcat_skey = pcat.pcat_skey  AND (pcat.end_unit_category = 'COMPONENT UNIT' OR PCAT.PRODUCT_CATEGORY = 'UNIT')
	inner join SHIP_TBL on SHIP_TBL.li_skey = f.li_skey


-- COMMAND ----------


CREATE OR REPLACE VIEW silver.PEL_ONESTREAM_PROF_WMS_XDOCK_ACTUAL_V 
AS select
CASE WHEN FISCAL_WK_OF_YR < 10 THEN 'FW0' || SUBSTRING(CAST(FISCAL_WK_OF_YR AS STRING),1,1)  ELSE  'FW' || SUBSTRING(CAST(FISCAL_WK_OF_YR AS STRING),1,2)  END FISCAL_WEEK,
'FY' || substring(CAST(FISCAL_YR_NUM AS STRING),1,4) FISCAL_YEAR,
PLANT_CODE ORG_CODE,
SHIP_TBL.shuttle_flg,
SHIP_TBL.ship_from_city,
rptd_fcpl.fp_order_type,
profisee_data_FP_Sub_Channel.MEMBER FP_SUB_CHANNEL_MEMBER_CODE,
CASE WHEN RPTD_FCPL.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL.FP_SUB_CHANNEL END FP_SUB_CHANNEL,
chan.fp_sub_channel chan_fpsc,
profisee_data_Branch_Account_Code.MEMBER BRANCH_ACCT_CODE_MEMBER_CODE,
chan.client_code BRANCH_ACCOUNT_CODE,
profisee_data_Sales_Segment.MEMBER SALES_SEGMENT_MEMBER_CODE,
NVL(sseg.SALES_SEGMENT,'NO SALES SEGMENT DATA') SALES_SEGMENT	,
profisee_data_Brand_Series.MEMBER BRAND_SERIES_MEMBER_CODE,
RPTD_FCPL.FP_BRAND_SERIES,
profisee_data_Product_Family.MEMBER PRODUCT_FAMILY_MEMBER_CODE,
PK.FP_PRODUCT	                                 ,
profisee_data_Product_Family.NAME  PRODUCT_FAMILY,
SUM(NVL(PURCHASE_QTY,0)) QTY,
DTM.FISCAL_YR_MONTH  FISCAL_YR_MONTH
from
(select  * from silver.edwproddb_edwprod_fact_agg_pqm_po_li_snapshot_fact where  PELLA_INV_DTM_SKEY > 20211115 
union
select * from silver.edwproddb_edwprod_fact_agg_ORA_po_li_snapshot_fact where  PELLA_INV_DTM_SKEY > 20211115 ) f
INNER JOIN silver.edwproddb_edwprod_dim_DATE_DIM DTM ON F.PELLA_INV_DTM_SKEY = DTM.DATE_SKEY
inner join silver.edwproddb_edwprod_dim_rptd_dim rptd on f.rptd_skey = rptd.rptd_skey
inner join silver.edwproddb_edwprod_dim_pcat_dim pcat on F.pcat_skey = pcat.pcat_skey  AND (pcat.end_unit_category = 'COMPONENT UNIT' OR PCAT.PRODUCT_CATEGORY = 'UNIT')
inner join silver.edwproddb_edwprod_dim_rptd_fcpl_dim rptd_fcpl on rptd.rptd_fcpl_skey = rptd_fcpl.rptd_fcpl_skey
INNER JOIN silver.edwproddb_edwprod_dim_PKDG_FCPL_DIM PK ON F.PKDG_FCPL_SKEY = PK.PKDG_FCPL_SKEY
inner join silver.edwproddb_edwprod_dim_plnt_dim plnt on F.plnt_skey = plnt.plnt_skey
left outer join silver.edwproddb_edwprod_EDW_EXT_PROFISEE_DOMAIN_CHAN_V chan on F.chan_skey = chan.chan_skey
left outer join silver.edwproddb_edwprod_EDW_EXT_PROFISEE_DOMAIN_SSEG_V sseg on f.sseg_skey = sseg.sseg_skey
inner join silver.PEL_ONESTREAM_PROF_WMS_XDOCK_PO_LI_SKEY_V ship_tbl on ship_tbl.po_li_skey = f.po_li_skey
INNER  JOIN silver.profisee_data_FP_Sub_Channel on chan.FP_SUB_CHANNEL = profisee_data_FP_Sub_Channel.SOURCE_ID  --INNER JOIN SO WE EXCLUDE NON DISTRIBUTOR ORDERS
LEFT OUTER JOIN silver.profisee_data_Branch_Account_Code  ON CLIENT_CODE = profisee_data_Branch_Account_Code.source_id
LEFT OUTER JOIN silver.profisee_data_Brand_Series  ON FP_BRAND_SERIES   = profisee_data_Brand_Series.source_id
LEFT OUTER JOIN silver.profisee_data_Product  ON FP_PRODUCT = profisee_data_Product.source_id
LEFT OUTER JOIN silver.profisee_data_Product_Family on profisee_data_Product.Product_Family = profisee_data_Product_Family.Code
LEFT OUTER JOIN silver.profisee_data_Sales_Segment  ON case when chan.FP_SUB_CHANNEL IN ('LOWES' ,'BIG BOX','PRO DEALER') THEN 'NO SALES SEGMENT DATA' else  nvl(sseg.SALES_SEGMENT,'NO SALES SEGMENT DATA') end = profisee_data_Sales_Segment.source_id

GROUP BY
CASE WHEN FISCAL_WK_OF_YR < 10 THEN 'FW0' || SUBSTRING(CAST(FISCAL_WK_OF_YR AS STRING),1,1)  ELSE  'FW' || SUBSTRING(CAST(FISCAL_WK_OF_YR AS STRING),1,2)  END ,
'FY' || substring(CAST(FISCAL_YR_NUM AS STRING),1,4) ,
PLANT_CODE ,
SHIP_TBL.shuttle_flg,
SHIP_TBL.ship_from_city,
rptd_fcpl.fp_order_type,
profisee_data_FP_Sub_Channel.MEMBER ,
CASE WHEN RPTD_FCPL.FP_SUB_CHANNEL = 'LOWES' THEN 'BIG BOX' ELSE RPTD_FCPL.FP_SUB_CHANNEL END ,
profisee_data_Branch_Account_Code.MEMBER ,
chan.client_code ,
profisee_data_Sales_Segment.MEMBER ,
NVL(sseg.SALES_SEGMENT,'NO SALES SEGMENT DATA')	,
profisee_data_Brand_Series.MEMBER ,
RPTD_FCPL.FP_BRAND_SERIES,
profisee_data_Product_Family.MEMBER ,
PK.FP_PRODUCT	                                 ,
profisee_data_Product_Family.NAME  ,
DTM.FISCAL_YR_MONTH ,
chan.fp_sub_channel


-- COMMAND ----------


CREATE OR REPLACE VIEW finance_silver.PEL_ONESTREAM_QB_AVA_TB_ACTUAL_V 
AS select 
'Actual' Scenario,
'FY' || substring(FILENAME,4,4) FISCAL_YEAR,
'FM' || DECODE(upper(substring(FILENAME,1,3)),'DEC','01','JAN','02','FEB','03','MAR','04','APR','05','MAY','06','JUN','07','JUL','08','AUG','09','SEP','10','OCT','11','NOV','12','NA') FISCAL_MONTH,
'401_501' ENTITY,
ACCOUNT,
DESCRIPTION,
ACCOUNT COST_CENTER,
'' SEGMENT,
'' ICP,
AMOUNT
from finance_silver.ftps_avanti_tb_trial_balance


-- COMMAND ----------


CREATE OR REPLACE VIEW finance_silver.PEL_ONESTREAM_QB_BON_TB_ACTUAL_V 
AS select 

'Actual' Scenario,
'FY' || substring(FILENAME,4,4) FISCAL_YEAR,
'FM' || DECODE(upper(substring(FILENAME,1,3)),'DEC','01','JAN','02','FEB','03','MAR','04','APR','05','MAY','06','JUN','07','JUL','08','AUG','09','SEP','10','OCT','11','NOV','12','NA') FISCAL_MONTH,
substring(filename,9,7)  ENTITY,
substring(account,1,instr(account,' ')-1) ACCOUNT,  --Account number is value upto the first space of the account field
substring(substring(account,instr(account,' ')+1),instr(substring(account,instr(account,' ')+1),' ')+1) DESCRIPTION,  --Description is value after the second space in account field
substring(account,1,instr(account,' ')-1) COST_CENTER,  --Account number is value upto the first space of the account field
'' SEGMENT,
'' ICP,
cast(replace(nvl(debitamount,0),',','') as double) - cast(replace(nvl(creditamount,0),',','') as double) AMOUNT

from finance_silver.ftps_bonelli_tb_trial_balance tb
where not(debitamount is null and creditamount is null)
and substring(substring(account,instr(account,' ')+1),instr(substring(account,instr(account,' ')+1),' ')+1) <> 'TOTAL'


-- COMMAND ----------


CREATE OR REPLACE VIEW finance_silver.PEL_ONESTREAM_QB_BUR_TB_ACTUAL_V 
AS select 
'Actual' Scenario,
'FY' || substring(FILENAME,4,4) FISCAL_YEAR,
'FM' || DECODE(upper(substring(FILENAME,1,3)),'DEC','01','JAN','02','FEB','03','MAR','04','APR','05','MAY','06','JUN','07','JUL','08','AUG','09','SEP','10','OCT','11','NOV','12','NA') FISCAL_MONTH,
substring(filename,9,7)  ENTITY,
substring(account,1,instr(account,' ')-1) ACCOUNT,  --Account number is value upto the first space of the account field
substring(substring(account,instr(account,' ')+1),instr(substring(account,instr(account,' ')+1),' ')+1) DESCRIPTION,  --Description is value after the second space in account field
substring(account,1,instr(account,' ')-1) COST_CENTER,  --Account number is value upto the first space of the account field
'' SEGMENT,
'' ICP,
cast(replace(nvl(debitamount,0),',','') as double) - cast(replace(nvl(creditamount,0),',','') as double) AMOUNT


from finance_silver.ftps_burris_tb_trial_balance 
where not(debitamount is null and creditamount is null)
and substring(substring(account,instr(account,' ')+1),instr(substring(account,instr(account,' ')+1),' ')+1) <> 'TOTAL'


-- COMMAND ----------


CREATE OR REPLACE VIEW finance_silver.PEL_ONESTREAM_QB_GRA_TB_ACTUAL_V 
AS select 

'Actual' Scenario,
'FY' || substring(FILENAME,4,4) FISCAL_YEAR,
'FM' || DECODE(upper(substring(FILENAME,1,3)),'DEC','01','JAN','02','FEB','03','MAR','04','APR','05','MAY','06','JUN','07','JUL','08','AUG','09','SEP','10','OCT','11','NOV','12','NA') FISCAL_MONTH,
substring(filename,9,7)  ENTITY,
substring(account,1,instr(account,' ')-1) ACCOUNT,  --Account number is value upto the first space of the account field
substring(substring(account,instr(account,' ')+1),instr(substring(account,instr(account,' ')+1),' ')+1) DESCRIPTION,  --Description is value after the second space in account field
substring(account,1,instr(account,' ')-1) COST_CENTER,  --Account number is value upto the first space of the account field
'' SEGMENT,
'' ICP,
cast(replace(nvl(debitamount,0),',','') as double) - cast(replace(nvl(creditamount,0),',','') as double) AMOUNT

from 
finance_silver.ftps_grabill_tb_trial_balance

where not(debitamount is null and creditamount is null)
and substring(substring(account,instr(account,' ')+1),instr(substring(account,instr(account,' ')+1),' ')+1) <> 'TOTAL'


-- COMMAND ----------


CREATE OR REPLACE VIEW gold.ebs_lowes_promo_orders_v 
AS select 
    h.order_number ebs_order_number,
    max(e.attribute_value) promo_pct ,
    h.ordered_date
    from 
    ebs_silver.oe_order_headers_all h,
    ebs_silver.oe_order_lines_all l,
    ebs_silver.pel_oe_order_lines_all_ext e
    where
        h.header_id = l.header_id
        and h.orig_sys_document_ref like 'L50%'
        and h.creation_date >   '2022-05-01'
        and l.line_id = e.line_id
        and e.attribute_name in ('SupplierPromoPercentage')  
        and h._change_oper<>'D' 
        and l._change_oper<>'D' 
        and nvl(e._change_oper,'I')<>'D'
    group by h.order_number, h.ordered_date


-- COMMAND ----------


