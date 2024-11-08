# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
import os

# COMMAND ----------

dbutils.widgets.text("SourceServerName","")
dbutils.widgets.text("SourceDatabaseName","")
dbutils.widgets.text("SourceSchemaName","")
dbutils.widgets.text("SourceExtractMethod","")
dbutils.widgets.text("UnityCatalogName","")
dbutils.widgets.text("StorageAccountName","")

# COMMAND ----------

source_server_name = dbutils.widgets.get("SourceServerName")
source_database_name = dbutils.widgets.get("SourceDatabaseName")
source_schema_name = dbutils.widgets.get("SourceSchemaName")
source_extract_method = dbutils.widgets.get("SourceExtractMethod")
unity_catlog_name = dbutils.widgets.get("UnityCatalogName")
storage_account_name = dbutils.widgets.get("StorageAccountName")

# Need to check later, if we want to continue with the gold tables (Medallion Architecture Changes)
if "_bronze" in unity_catlog_name:  
    unity_catalog_name = unity_catlog_name.replace("_bronze", "_gold")
else:
    unity_catalog_name = unity_catlog_name.replace("_silver", "_gold")

uc_schema = source_database_name.lower()

abfss_path = f"abfss://master@{storage_account_name}.dfs.core.windows.net/"

# This variable is used for creating bronze/silver tables.
bronze_table_name = unity_catlog_name + '.' + uc_schema + '.' + source_schema_name + '_' 
gold_delta_table_path = abfss_path + 'gold/' + source_server_name+'/'+source_database_name+'/' + source_schema_name + '/' 
gold_table_name = unity_catalog_name + "." + uc_schema + '.' + source_schema_name + '_'

# COMMAND ----------

clientsgroups = bronze_table_name+'ClientsGroups'
MetaProductAttrMarkup = bronze_table_name+'MetaProductAttrMarkup_type2'
MetaProductMarkup = bronze_table_name+'MetaProductMarkup_type2'
MetaGlobalMarkup = bronze_table_name+'MetaGlobalMarkup_type2'
MetaMarkupEffectiveDate = bronze_table_name+'MetaMarkupEffectiveDate'
BusinessSegments = bronze_table_name+'BusinessSegments'
Clients = bronze_table_name+'Clients'

# COMMAND ----------

gm_query = "SELECT ClientsGroups.ClientGroup, clientsgroups.ClientName, BusinessSegments.Name as Segment, MetaGlobalMarkup.DefaultMarkup, MetaGlobalMarkup.ManuallyPricedLine,MetaGlobalMarkup.OneTimeSetupMult1, MetaGlobalMarkup.OneTimeSetupAdder, MetaGlobalMarkup.OneTimeSetupMult2,MetaGlobalMarkup.PerUnitMult1, MetaGlobalMarkup.PerUnitAdder, MetaGlobalMarkup.PerUnitMult2,MetaGlobalMarkup.WrappingMult1, MetaGlobalMarkup.WrappingAdder, MetaGlobalMarkup.WrappingMult2,MetaGlobalMarkup.UnitBranchMult1, MetaGlobalMarkup.UnitBranchAdder, MetaGlobalMarkup.UnitBranchMult2,MetaGlobalMarkup.UnitPellaMult1, MetaGlobalMarkup.UnitPellaAdder, MetaGlobalMarkup.UnitPellaMult2,MetaGlobalMarkup.AccessoryMult1, MetaGlobalMarkup.AccessoryAdder, MetaGlobalMarkup.AccessoryMult2,MetaGlobalMarkup.BranchCatMaterialMult, MetaGlobalMarkup.BranchCatLaborMult, MetaGlobalMarkup.PellaCatMaterialMult,MetaGlobalMarkup.OutOfUnitMult1, MetaGlobalMarkup.OutOfUnitAdder, MetaGlobalMarkup.OutOfUnitMult2,MetaGlobalMarkup.EffectiveDate, MetaGlobalMarkup.CreatedDate, MetaGlobalMarkup.LastModified,MetaGlobalMarkup.DL_ARRIVAL_DTT,MetaGlobalMarkup.DL_UPDATE_DTT FROM {} as MetaMarkupEffectiveDate,{} as MetaGlobalMarkup ,{} as clientsgroups,{} as Clients,{} as BusinessSegments where MetaGlobalMarkup.ClientID == MetaMarkupEffectiveDate.ClientID and MetaGlobalMarkup.ClientID == clientsgroups.ClientID and Clients.ClientID == ClientsGroups.Clientid and MetaGlobalMarkup.Segment == BusinessSegments.ID and Clientsgroups.islive = 1 and MetaMarkupEffectiveDate.ClientID = MetaGlobalMarkup.ClientID and MetaMarkupEffectiveDate.EffectiveDate = MetaGlobalMarkup.EffectiveDate and (MetaGlobalMarkup.Flag = 1 or MetaGlobalMarkup.DL_UPDATE_DTT > add_months(current_timestamp(),-1))".format(MetaMarkupEffectiveDate,MetaGlobalMarkup,clientsgroups,Clients,BusinessSegments)

# COMMAND ----------

pm_query = "SELECT ClientsGroups.ClientGroup,   ClientsGroups.ClientName, CLients.Clientid, BusinessSegments.Name as Segment, SeqNum, Type, Brand, Product,MetaProductMarkup.EffectiveDate, PellaAssembledMult1, PellaAssembledAdder,PellaAssembledMult2, BranchAssembledMult1, BranchAssembledAdder,BranchAssembledMult2, CreatedDate,MetaProductMarkup.LastModified, MetaProductMarkup.Deleted,MetaProductMarkup.DL_ARRIVAL_DTT,MetaProductMarkup.DL_UPDATE_DTT FROM {} as MetaMarkupEffectiveDate,{} as MetaProductMarkup,{} as clientsgroups,{} as Clients,{} as BusinessSegments WHERE MetaProductMarkup.ClientID == MetaMarkupEffectiveDate.ClientID and MetaProductMarkup.ClientID == clientsgroups.ClientID and Clients.ClientID == ClientsGroups.Clientid and MetaProductMarkup.Segment == BusinessSegments.ID and MetaMarkupEffectiveDate.ClientID = MetaProductMarkup.ClientID and MetaMarkupEffectiveDate.EffectiveDate =  MetaProductMarkup.EffectiveDate and (MetaProductMarkup.Flag = 1 or MetaProductMarkup.DL_UPDATE_DTT > add_months(current_timestamp(),-1))".format(MetaMarkupEffectiveDate,MetaProductMarkup,clientsgroups,Clients,BusinessSegments)

# COMMAND ----------

pam_query = "SELECT ClientsGroups.ClientGroup, ClientsGroups.ClientName,BusinessSegments.Name as Segment, SeqNum, Type, Brand, Product, AttributeQuestion,AttributeAnswer,MetaProductAttrMarkup.EffectiveDate,PellaAssembledMult1,PellaAssembledAdder,PellaAssembledMult2,BranchAssembledMult1,BranchAssembledAdder, BranchAssembledMult2, CreatedDate, MetaProductAttrMarkup.LastModified, MetaProductAttrMarkup.Deleted,MetaProductAttrMarkup.DL_ARRIVAL_DTT,MetaProductAttrMarkup.DL_UPDATE_DTT FROM {} as MetaMarkupEffectiveDate, {} as MetaProductAttrMarkup,{} as clientsgroups,{} as Clients,{} as BusinessSegments where MetaProductAttrMarkup.ClientID = MetaMarkupEffectiveDate.ClientID and MetaProductAttrMarkup.ClientID = clientsgroups.ClientID and Clients.ClientID = ClientsGroups.Clientid and MetaProductAttrMarkup.Segment = BusinessSegments.ID and MetaMarkupEffectiveDate.ClientID = MetaProductAttrMarkup.ClientID and MetaMarkupEffectiveDate.EffectiveDate =  MetaProductAttrMarkup.EffectiveDate and  (MetaProductAttrMarkup.Flag = 1 or MetaProductAttrMarkup.DL_UPDATE_DTT > add_months(current_timestamp(),-1))".format(MetaMarkupEffectiveDate, MetaProductAttrMarkup,clientsgroups,Clients,BusinessSegments)

# COMMAND ----------

gm = spark.sql(gm_query)
pm = spark.sql(pm_query)
pam = spark.sql(pam_query)

# COMMAND ----------

if source_extract_method == 'FULL' and DeltaTable.isDeltaTable(spark, gold_delta_table_path+ 'GlobalMarkups/'):
  spark.sql('Truncate Table ' + gold_table_name + 'GlobalMarkups' )
  gm.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(gold_delta_table_path + 'GlobalMarkups/') 
else:
  print("creating table")
  gm.write.format('delta').mode('append').save(gold_delta_table_path+'GlobalMarkups/')
  spark.sql('CREATE TABLE IF NOT EXISTS ' + gold_table_name+ 'GlobalMarkups using DELTA LOCATION "' + gold_delta_table_path +'GlobalMarkups/" ')

  print('CREATE TABLE IF NOT EXISTS ' + gold_table_name+ 'GlobalMarkups using DELTA LOCATION "' + gold_delta_table_path +'GlobalMarkups/" ')

# COMMAND ----------

if source_extract_method == 'FULL' and DeltaTable.isDeltaTable(spark, gold_delta_table_path+ 'ProductMarkups/'):
  spark.sql('Truncate Table ' + gold_table_name + 'ProductMarkups')
  pm.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(gold_delta_table_path + 'ProductMarkups/')   
else:
  print("creating table")
  pm.write.format('delta').mode('append').save(gold_delta_table_path +'ProductMarkups/')
  spark.sql('CREATE TABLE IF NOT EXISTS ' + gold_table_name + 'ProductMarkups using DELTA LOCATION "' + gold_delta_table_path +'ProductMarkups/" ')

  print('CREATE TABLE IF NOT EXISTS ' + gold_table_name + 'ProductMarkups using DELTA LOCATION "' + gold_delta_table_path +'ProductMarkups/" ')

# COMMAND ----------

if source_extract_method == 'FULL' and DeltaTable.isDeltaTable(spark, gold_delta_table_path+ 'ProductAttrMarkups/'):
  spark.sql('Truncate Table ' + gold_table_name + 'ProductAttrMarkups' )
  pam.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(gold_delta_table_path +'ProductAttrMarkups/')   
else:
  print("creating table")
  pam.write.format('delta').mode('append').save(gold_delta_table_path+'ProductAttrMarkups/')
  spark.sql('CREATE TABLE IF NOT EXISTS ' + gold_table_name+ 'ProductAttrMarkups using DELTA LOCATION "' + gold_delta_table_path +'ProductAttrMarkups/" ')
            
  print('CREATE TABLE IF NOT EXISTS ' + gold_table_name+ 'ProductAttrMarkups using DELTA LOCATION "' + gold_delta_table_path +'ProductAttrMarkups/" ')
