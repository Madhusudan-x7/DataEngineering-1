# Databricks notebook source
spark.sql('create database if not exists silver COMMENT "silver" Location "/mnt/master/silver/"')

# COMMAND ----------

# dbutils.widgets.text("DataLakeSourcePath","")
dbutils.widgets.text("SourceKeyColumnList","")
dbutils.widgets.text("SourceTablePath","")
dbutils.widgets.text("SourceTableName","")
dbutils.widgets.text("SourceServerName","")
dbutils.widgets.text("SourceDatabaseName","")
dbutils.widgets.text("SourceSchemaName","")
dbutils.widgets.text("SourceExtractMethod","")
dbutils.widgets.text("DeltaUpdateWatermarkColumnName","")
dbutils.widgets.text("HistoryLoadInd","")


# COMMAND ----------

# DBTITLE 1,setting Input variable values
# DetlaTablePath = '/mnt/master/'+ dbutils.widgets.get("DataLakeSourcePath")
SourceTableName = dbutils.widgets.get("SourceTableName")
SourceKeyColumnListvalue = dbutils.widgets.get("SourceKeyColumnList")
SourceTablePath = '/mnt/master/' +dbutils.widgets.get("SourceTablePath")
SourceServerName = dbutils.widgets.get("SourceServerName")
SourceDatabaseName = dbutils.widgets.get("SourceDatabaseName")
SourceSchemaName = dbutils.widgets.get("SourceSchemaName")
SourceExtractMethod = dbutils.widgets.get("SourceExtractMethod")
DeltaUpdateWatermarkColumnName = dbutils.widgets.get("DeltaUpdateWatermarkColumnName")
HistoryLoadInd = dbutils.widgets.get("HistoryLoadInd")

SilverDetlaTablePath = '/mnt/master/silver/'+ SourceServerName+'/'+SourceDatabaseName+'/' + SourceSchemaName + '/' + SourceTableName + '/' 
SilverTableName = SourceServerName + '_' + SourceDatabaseName + '_' + SourceSchemaName + '_' + SourceTableName


# COMMAND ----------

# DBTITLE 1,Creating Join Condition to merge table
SourceKeyColumnList = SourceKeyColumnListvalue.split(',')
for key in SourceKeyColumnList:
  joincondition =  'original.' + key +'= updates.' + key + ' '

# COMMAND ----------

# DBTITLE 1,Create and Load tables 
# +If table not exists create delta table
# For Full Load Truncate Table and insert the complete data
# For delta table Merge the table base

from delta.tables import *
from pyspark.sql.functions import *
import os
updatesDF= spark.read.parquet('dbfs:' + SourceTablePath + '*.parquet')  
if SourceExtractMethod == 'FULL' and os.path.exists('/dbfs' + SilverDetlaTablePath):
  spark.sql('Truncate Table silver.' + SilverTableName )
  updatesDF.write.format('delta').mode('append').save('dbfs:' +SilverDetlaTablePath)
elif os.path.exists('/dbfs' + SilverDetlaTablePath) and DeltaTable.isDeltaTable(spark, 'dbfs:'+SilverDetlaTablePath):
  if HistoryLoadInd == 'Y':
    updatesDF.withColumn("File_Date", date_format(DeltaUpdateWatermarkColumnName, "yyyyMM")).write.partitionBy('File_Date').format('delta').mode('append').save('dbfs:' +SilverDetlaTablePath)
  else:
    updatesDF = updatesDF.withColumn("File_Date", date_format(DeltaUpdateWatermarkColumnName, "yyyyMM"))
    deltaTable = DeltaTable.forPath(spark,'dbfs:'+SilverDetlaTablePath)
    deltaTable.alias('original').merge(
        updatesDF.alias('updates'),
        joincondition)\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
  if SourceExtractMethod == 'FULL':
    updatesDF.write.format('delta').mode('append').save('dbfs:' +SilverDetlaTablePath)
    spark.sql('CREATE TABLE IF NOT EXISTS silver.' + SilverTableName+ ' using DELTA LOCATION "dbfs:' + SilverDetlaTablePath +'" ')
  else:
    #     updatesDF.write.partitionBy(date_format(DeltaUpdateWatermarkColumnName ,'yyyMM')).format('delta').mode('append').save('dbfs:' +SilverDetlaTablePath)
    updatesDF.withColumn("File_Date", date_format(DeltaUpdateWatermarkColumnName, "yyyyMM")).write.partitionBy('File_Date').format('delta').mode('append').save('dbfs:' +SilverDetlaTablePath)
    spark.sql('CREATE TABLE IF NOT EXISTS silver.' + SilverTableName+ ' using DELTA LOCATION "dbfs:' + SilverDetlaTablePath +'" ')
    spark.sql('OPTIMIZE silver.' + SilverTableName + ' ZORDER BY (' + SourceKeyColumnListvalue + ')')
