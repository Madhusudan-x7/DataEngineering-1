-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("SourceCatalog","")
-- MAGIC dbutils.widgets.text("TargetCatalog","")

-- COMMAND ----------

CREATE OR REPLACE VIEW $TargetCatalog.anaplan.vw_forecast_daily AS 
WITH LAST_THREE_YYMM_RECORDS AS (
  SELECT
    FCST_Year,
    FCST_Month,
    ROW_NUMBER() OVER (
      ORDER BY
        FORECAST.FCST_Year DESC,
        FORECAST.FCST_Month DESC
    ) AS LAG
  FROM
    $SourceCatalog.anaplan.forecast
  GROUP BY
    FCST_Year,
    FCST_Month
  ORDER BY
    FCST_Year DESC,
    FCST_Month DESC
  LIMIT
    3
), ORDERED_RECORDS AS (
  SELECT
    (REQUIRED_YY_MM.LAG -1) AS LAG,
    FORECAST.*,
    CONCAT(20, RIGHT(time, 2)) F_YEAR,
    SUBSTRING(time, 5, 3) F_WEEK,
    RANK() OVER(
      PARTITION BY FORECAST.FCST_Year,
      FORECAST.FCST_Month
      ORDER BY
        INS_GMTS DESC
    ) AS PRIORITY
  FROM
    $SourceCatalog.anaplan.forecast FORECAST
    INNER JOIN LAST_THREE_YYMM_RECORDS REQUIRED_YY_MM ON FORECAST.FCST_Month = REQUIRED_YY_MM.FCST_Month
    AND FORECAST.FCST_YEAR = REQUIRED_YY_MM.FCST_YEAR
),
PRIORITY_RECORDS AS (
  SELECT
    *
  FROM
    ORDERED_RECORDS
  WHERE
    PRIORITY = 1
)
SELECT
  anaplan_forecast.FCST_YEAR,
  anaplan_forecast.FCST_MONTH,
  anaplan_forecast.LAG,
  anaplan_forecast.TIME,
  anaplan_forecast.INS_GMTS,
  anaplan_forecast.BUSINESS_CODE,
  anaplan_forecast.Brand_Series,
  anaplan_forecast.Brand_Series_Code,
  anaplan_forecast.Product,
  anaplan_forecast.Product_Code,
  anaplan_forecast.FP_Sub_Channel,
  anaplan_forecast.FP_Sub_Channel_Code,
  anaplan_forecast.Account_Branch,
  anaplan_forecast.Account_Branch_Code,
  anaplan_forecast.Sales_Segment,
  anaplan_forecast.Sales_Segment_Code,
  anaplan_forecast.Plant,
  anaplan_forecast.Plant_Code,
  anaplan_forecast.Order_Type,
  anaplan_forecast.Door_Panel_Quantity,
  anaplan_forecast.FILENAME,
  anaplan_forecast.Segment_Code,
  anaplan_forecast.Product_Family,
  anaplan_forecast.Product_Family_Code,
  anaplan_forecast.Region,
  anaplan_forecast.Region_Code,
  sum(anaplan_forecast.quantity / business_day.business_day_wk_count) Quantity,
  sum(anaplan_forecast.revenue / business_day.business_day_wk_count) Revenue,
  'FY' || cast(date_dim.fiscal_yr_num as integer) FISCAL_YEAR,
  'FM' || lpad(
    cast(date_dim.fiscal_month_of_yr as integer),
    2,
    '0'
  ) FISCAL_MONTH
FROM
  PRIORITY_RECORDS anaplan_forecast
  INNER JOIN $SourceCatalog.pel_edwprod.dim_date_dim date_dim ON anaplan_forecast.F_YEAR = date_dim.FISCAL_YR_NUM
  AND anaplan_forecast.F_WEEK = date_dim.FISCAL_WK_OF_YR
  INNER JOIN (
    SELECT
      WK_DAY.FISCAL_YR_NUM,
      WK_DAY.FISCAL_WK_OF_YR,
      COUNT(*) business_day_wk_count
    FROM
      $SourceCatalog.pel_edwprod.dim_date_dim wk_day
    WHERE
      wk_day.transaction_holiday = 'Business'
    GROUP BY
      WK_DAY.FISCAL_YR_NUM,
      WK_DAY.FISCAL_WK_OF_YR
  ) business_day ON anaplan_forecast.F_YEAR = business_day.FISCAL_YR_NUM
  AND anaplan_forecast.F_WEEK = business_day.FISCAL_WK_OF_YR
WHERE
  date_dim.transaction_holiday = 'Business'
GROUP BY
  anaplan_forecast.FCST_YEAR,
  anaplan_forecast.FCST_MONTH,
  anaplan_forecast.LAG,
  anaplan_forecast.TIME,
  anaplan_forecast.INS_GMTS,
  anaplan_forecast.BUSINESS_CODE,
  anaplan_forecast.Brand_Series,
  anaplan_forecast.Brand_Series_Code,
  anaplan_forecast.Product,
  anaplan_forecast.Product_Code,
  anaplan_forecast.FP_Sub_Channel,
  anaplan_forecast.FP_Sub_Channel_Code,
  anaplan_forecast.Account_Branch,
  anaplan_forecast.Account_Branch_Code,
  anaplan_forecast.Sales_Segment,
  anaplan_forecast.Sales_Segment_Code,
  anaplan_forecast.Plant,
  anaplan_forecast.Plant_Code,
  anaplan_forecast.Order_Type,
  anaplan_forecast.Door_Panel_Quantity,
  anaplan_forecast.FILENAME,
  anaplan_forecast.Segment_Code,
  anaplan_forecast.Product_Family,
  anaplan_forecast.Product_Family_Code,
  anaplan_forecast.Region,
  anaplan_forecast.Region_Code,
  'FY' || cast(date_dim.fiscal_yr_num as integer),
  'FM' || lpad(
    cast(date_dim.fiscal_month_of_yr as integer),
    2,
    '0'
  );

