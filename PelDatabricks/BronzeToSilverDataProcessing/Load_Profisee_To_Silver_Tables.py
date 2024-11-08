# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from delta.tables import *
import pandas as pd
import os
import os.path

# COMMAND ----------

dbutils.widgets.text("pProfiseeHostname","")
dbutils.widgets.text("pProfiseeDatabase","")
dbutils.widgets.text("pProfiseeUsername","")
dbutils.widgets.text("pProfiseePasswordKey","")
dbutils.widgets.text("pDatabricksScope","")
dbutils.widgets.text("pAzureSQLServerName","")
dbutils.widgets.text("pAzureSQLDatabaseName","")
dbutils.widgets.text("pAzureSQLUserName","")
dbutils.widgets.text("pAzureSQLPasswordKey","")

# COMMAND ----------

profiseeHostname=dbutils.widgets.get('pProfiseeHostname')
profiseeDatabase=dbutils.widgets.get('pProfiseeDatabase')
profiseeUsername=dbutils.widgets.get('pProfiseeUsername')
profiseePasswordKey=dbutils.widgets.get('pProfiseePasswordKey')
databricksScope=dbutils.widgets.get('pDatabricksScope')
controlTableHostname=dbutils.widgets.get('pAzureSQLServerName')
controlTableDatabase=dbutils.widgets.get('pAzureSQLDatabaseName')
controlTableUsername=dbutils.widgets.get('pAzureSQLUserName')
controlTablePasswordKey=dbutils.widgets.get('pAzureSQLPasswordKey')

# COMMAND ----------

# Python code to connect to Azure SQL Databases from Azure Databricks with Screts Scope
# Declare variables for creating profisee URL
profiseePort = 1433 # Replace with your SQL Server port number

# Connection secrets from vault
profiseePassword = dbutils.secrets.get(scope=databricksScope,key=profiseePasswordKey) # Replace the scope and key accordingly

# Create profisee URL
profiseeUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(profiseeHostname, profiseePort, profiseeDatabase)
profiseeConnectionProperties = {
  "user" : profiseeUsername,
  "password" : profiseePassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Declare variables for creating controlTable URL
controlTablePort = 1433 # Replace with your SQL Server port number

# Connection secrets from vault
controlTablePassword = dbutils.secrets.get(scope=databricksScope,key=controlTablePasswordKey) # Replace the scope and key accordingly

# Create controlTable URL
controlTableUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(controlTableHostname, controlTablePort, controlTableDatabase)

controlConnectionProperties = {
  "user" : controlTableUsername,
  "password" : controlTablePassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInWrite','LEGACY')

#controlTableQuery = "(select * from etl.ControlTableProfisee where IsActive = 'Y') as ControlTable"
controlTableQuery = "(select ctp.* ,utc.UnityCatalogName,utc.StorageAccountName from etl.ControlTableProfisee ctp inner join etl.UnityCatalogConnectionStore utc on ctp.isActive = utc.IsActiveFlag and ctp.UCFlag = 'Y' and utc.IsActiveFlag='Y') as ControlTable"

controlTable = spark.read.jdbc(url=controlTableUrl, table=controlTableQuery, properties=controlConnectionProperties)

# Iterate through control table
for row in controlTable.collect():

  abfss_path = f"abfss://{row.DataLakeDestinationContainer}@{row.StorageAccountName}.dfs.core.windows.net/silver/"

  properCatalogName = row.UnityCatalogName
  properCatalogName = properCatalogName.replace('bronze','silver')

  create_schema_sql = f"Create schema IF NOT EXISTS {properCatalogName}.{row.ProfiseeDatabaseName.lower()}"
  spark.sql(create_schema_sql)

  #SilverDeltaTablePath = '/mnt/master/silver/' + row.ProfiseeDatabaseName + '/' + row.ProfiseeSchemaName + '/' + row.ProfiseeTableName + '/'
  SilverDeltaTablePath = abfss_path + row.ProfiseeDatabaseName + '/' + row.ProfiseeSchemaName + '/' + row.ProfiseeTableName + '/'
  
  #SilverTableName = row.ProfiseeDatabaseName + '_' + row.ProfiseeSchemaName + '_' + row.ProfiseeTableName
  #unity_catlog_name = row.UnityCatalogName
  uc_schema = row.ProfiseeDatabaseName.lower()
  SilverTableName = row.ProfiseeTableName.lower()

  profiseeQuery = "(" + row.ProfiseeEntityQueryExport + ")"" as " + SilverTableName 

  profiseeResult = spark.read.jdbc(url=profiseeUrl, table=profiseeQuery, properties=profiseeConnectionProperties)

  # To-DO Needs to be handled via config source params
  if row.ProfiseeEntityName.lower()=='division':
      profiseeResult=profiseeResult.withColumnRenamed('Segment 1','Segment_1').withColumnRenamed('Division Description','Division_Description')
  
  #profiseeResult.write.format('delta').mode('overwrite').option("mergeSchema","true").save('dbfs:' +SilverDeltaTablePath)
  profiseeResult.write.format('delta').mode('overwrite').option("mergeSchema","true").save(SilverDeltaTablePath)

  #spark.sql('CREATE TABLE IF NOT EXISTS silver.' + SilverTableName+ ' using DELTA LOCATION "dbfs:' + SilverDeltaTablePath +'" ')
  spark.sql("CREATE TABLE IF NOT EXISTS " + properCatalogName + '.' + uc_schema + '.' + SilverTableName + ' using DELTA LOCATION "' + SilverDeltaTablePath + '" ')
    
  #writing a dataframe to csv
  SilverProfiseeCSVFileName = row.ProfiseeTableName
  SilverProfiseeCSVPath = '/dbfs/mnt/master/silver/Profisee/csv/' + SilverProfiseeCSVFileName +  '.csv'
  #SilverProfiseeCSVPath = abfss_path + row.ProfiseeDatabaseName + '/csv/' + SilverProfiseeCSVFileName +  '.csv'
  print(SilverProfiseeCSVPath)
  
  #profiseeResult.write.mode("overwrite").option("header",True).option("delimiter","|").csv(SilverProfiseeCSVPath)

  #converting to pandas intentionally due to spark limitation of file name
  pd = profiseeResult.toPandas()
  pd.to_csv(SilverProfiseeCSVPath,header=True,sep='|',index=False)

# COMMAND ----------


