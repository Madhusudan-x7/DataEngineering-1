# Databricks notebook source
dbutils.widgets.text("UnityCatalogName","")

# COMMAND ----------

UnityCatalogName = dbutils.widgets.get('UnityCatalogName')
print(UnityCatalogName)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_silver.gp_reily.dbo_gl00100

# COMMAND ----------

df = spark.sql(f"select * from {UnityCatalogName}_silver.gp_reily.dbo_GL00100")


# COMMAND ----------

# DBTITLE 1,onestream.gp_bat_tb_actual_v
create_view_stmt = """CREATE OR REPLACE VIEW {}_gold.onestream.gp_bat_tb_actual_v AS SELECT 
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
select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
from {}_silver.gp_bat.dbo_gl10110 g  --gl_summary_data
  inner join {}_silver.gp_bat.dbo_GL00100 b  
    on   g.ACTINDX = b.ACTINDX
  inner join {}_silver.gp_bat.dbo_GL00102 c --categories
    on g.ACCATNUM = c.ACCATNUM
  inner join {}_silver.gp_bat.dbo_GL00105 a --for account number
    on g.ACTINDX = a.ACTINDX
where b.ACCTTYPE = 1 AND PERIODID <= 1
group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
""".format(UnityCatalogName)

print(create_view_stmt)


# COMMAND ----------

# DBTITLE 1,Execute onestream.gp_bat_tb_actual_v
dfResults = spark.sql(create_view_stmt)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace VIEW {}_silver.pel_onestream_gp_rei_tb_actual_v
# MAGIC AS SELECT 
# MAGIC 'Actual' Scenario,
# MAGIC concat('FY',YEAR1) FISCAL_YEAR,
# MAGIC CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
# MAGIC substring(ACTNUMST,1,8) Entity,
# MAGIC substring(ACTNUMST,5,4) Account,
# MAGIC ACTDESCR Description,
# MAGIC substring(ACTNUMST,10,3) Cost_Center,
# MAGIC substring(ACTNUMST,14,3) Segment,
# MAGIC '' ICP,
# MAGIC sum(PERIOD_BALANCE) Amount,
# MAGIC 'peohmsgp01_bat' SOURCE,
# MAGIC TRIM(ACTNUMST) ACTNUMST
# MAGIC FROM 
# MAGIC (
# MAGIC select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 1
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC UNION
# MAGIC select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 2
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC UNION
# MAGIC select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 3
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 4
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 5
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 6
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 7
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
# MAGIC select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 8
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 9
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 10
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <=11
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
# MAGIC select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.rwwsmdgp01_reillygp_dbo_gl10110 g  
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00102 c 
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.rwwsmdgp01_reillygp_dbo_GL00105 a 
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 12
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC ) GL
# MAGIC GROUP BY 
# MAGIC year1, periodid, ACTNUMST, ACTDESCR

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW {}_silver.PEL_ONESTREAM_GP_BIR_TB_ACTUAL_V 
# MAGIC AS SELECT 
# MAGIC
# MAGIC 'Actual' Scenario,
# MAGIC concat('FY',YEAR1) FISCAL_YEAR,
# MAGIC CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
# MAGIC substring(ACTNUMST,1,8) Entity,
# MAGIC substring(ACTNUMST,5,4) Account,
# MAGIC ACTDESCR Description,
# MAGIC substring(ACTNUMST,10,3) Cost_Center,
# MAGIC substring(ACTNUMST,14,3) Segment,
# MAGIC '' ICP,
# MAGIC sum(PERIOD_BALANCE) Amount,
# MAGIC ' peohmsgp01_birm' SOURCE,
# MAGIC TRIM(ACTNUMST) ACTNUMST
# MAGIC FROM 
# MAGIC (
# MAGIC select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 1
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC UNION
# MAGIC select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 2
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC UNION
# MAGIC select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 3
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 4
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 5
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 6
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 7
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
# MAGIC select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 8
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 9
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 10
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <=11
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
# MAGIC select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.peohmsgp01_birm_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.peohmsgp01_birm_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 12
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC ) GL
# MAGIC GROUP BY 
# MAGIC year1, periodid, ACTNUMST, ACTDESCR

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW {}_silver.PEL_ONESTREAM_GP_WCL_TB_ACTUAL_V 
# MAGIC AS SELECT 
# MAGIC
# MAGIC 'Actual' Scenario,
# MAGIC concat('FY',YEAR1) FISCAL_YEAR,
# MAGIC CONCAT('FM',LPAD(PERIODID,2,'0')) FISCAL_MONTH,
# MAGIC substring(ACTNUMST,1,8) Entity,
# MAGIC substring(ACTNUMST,5,4) Account,
# MAGIC ACTDESCR Description,
# MAGIC substring(ACTNUMST,10,3) Cost_Center,
# MAGIC substring(ACTNUMST,14,3) Segment,
# MAGIC '' ICP,
# MAGIC sum(PERIOD_BALANCE) Amount,
# MAGIC ' njnhmsgp01_pwdnj' SOURCE,
# MAGIC TRIM(ACTNUMST) ACTNUMST
# MAGIC FROM 
# MAGIC (
# MAGIC select '01' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 1
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC UNION
# MAGIC select '02' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 2
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC UNION
# MAGIC select '03' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 3
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '04' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 4
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '05' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 5
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '06' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 6
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '07' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 7
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
# MAGIC select '08' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 8
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '09' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 9
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '10' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 10
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC union
# MAGIC select '11' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <=11
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC union
# MAGIC select '12' AS PERIODID, MAX(PERIODID), g.YEAR1 ,  a.ACTNUMST , b.ACTDESCR ,c.ACCATDSC , SUM(g.DEBITAMT-g.CRDTAMNT) PERIOD_BALANCE
# MAGIC from {}_silver.njnhmsgp01_pwdnj_dbo_gl10110 g  --gl_summary_data
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00100 b  
# MAGIC     on   g.ACTINDX = b.ACTINDX
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00102 c --categories
# MAGIC     on g.ACCATNUM = c.ACCATNUM
# MAGIC   inner join {}_silver.njnhmsgp01_pwdnj_dbo_GL00105 a --for account number
# MAGIC     on g.ACTINDX = a.ACTINDX
# MAGIC where b.ACCTTYPE = 1 AND PERIODID <= 12
# MAGIC group by g.YEAR1 ,a.ACTNUMST ,b.ACTDESCR ,c.ACCATDSC 
# MAGIC ) GL
# MAGIC GROUP BY 
# MAGIC year1, periodid, ACTNUMST, ACTDESCR
