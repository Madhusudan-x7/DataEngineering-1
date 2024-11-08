# Databricks notebook source
# MAGIC %md # Notebook Set Up

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

# DBTITLE 1,Get Set Parmeters
dbutils.widgets.text("SilverDatabase","")
dbutils.widgets.text("ScriptTable","")
dbutils.widgets.text("TablePath","")
dbutils.widgets.text("BackupSchema","")

# SilverDatabase = ebs_silver,wms_silver,silver,etl_silver,dms_silver,oms_silver,poms_silver
# ScriptTable = create_statements_data
# TablePath = /mnt/master/user/shan/create_statements
# BackupSchema = default

silver_database = dbutils.widgets.get("SilverDatabase")
script_table = dbutils.widgets.get("ScriptTable")
table_path = dbutils.widgets.get("TablePath")
backup_schema = dbutils.widgets.get("BackupSchema")

table_name = f"{backup_schema}.{script_table}"

final_table_path = f"{table_path}/{script_table}_final/data"

final_table_name = f"{backup_schema}.{script_table}_final"

# COMMAND ----------

# MAGIC %md ### Function Imports

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import col, max, last,to_date, broadcast, length, lit, row_number, when, lead, to_timestamp, to_utc_timestamp, concat,expr, regexp_extract, substring_index
import datetime
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.types import NullType

# COMMAND ----------

# MAGIC %md ### Utility Functions

# COMMAND ----------

# DBTITLE 1,Function to drop Silver Table
def drop_table(Query: str) -> str:
    z = ""
    drop_tble = str(Query).replace('silver','bronze')
    try:
        show_query = spark.sql(drop_tble)
        x = "Table dropped Successfully"
        return x
    
    except Exception as e:
        x = "Table not dropped"
        return x

# COMMAND ----------

# DBTITLE 1,Function to create Bronze Table
def create_table(Query: str) -> str:
    z = ""

    create_tble = str(Query)

    try:
        show_query = spark.sql(create_tble)
        x = "Table created Successfully"
        return x
    
    except Exception as e:
        x = "Table not created"
        return x

# COMMAND ----------

# DBTITLE 1,Read Backup Script table
table_df = spark.sql(f"""select case when create_stmt like '%PARTITIONED%' then concat(substr(create_stmt,1,CHARINDEX('USING', create_stmt)-1) ,' USING delta ',substr(create_stmt,CHARINDEX('LOCATION', create_stmt))) else create_stmt end create_statement,
database,tableName,isTemporary,Query,DropQuery,stmt from (
Select concat(SUBSTRING(stmt, CHARINDEX('CREATE TABLE', stmt),CHARINDEX('(', stmt)-1),SUBSTRING(stmt, CHARINDEX('USING delta', stmt),CHARINDEX('TBLPROPERTIES', stmt)-CHARINDEX('USING delta', stmt))) create_stmt,* from {table_name})""")
display(table_df)

# COMMAND ----------

# DBTITLE 1,Create tables
# table_df = spark.sql(f"Select * from {table_name}")

panas_df=table_df.toPandas()

panas_df['drop_table']=panas_df['DropQuery'].apply(lambda x: drop_table(x))
panas_df['status']=panas_df['create_statement'].apply(lambda x: create_table(x))

display(panas_df)

final_df = spark.createDataFrame(panas_df)
final_df.write.format("delta").mode("overwrite").saveAsTable(final_table_name, path = final_table_path )
