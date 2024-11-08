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
# ScriptTable = create_statements_data_std
# TablePath = /mnt/master/user/create_statements
# BackupSchema = default

silver_database = dbutils.widgets.get("SilverDatabase")
script_table = dbutils.widgets.get("ScriptTable")
table_path = dbutils.widgets.get("TablePath")
backup_schema = dbutils.widgets.get("BackupSchema")

table_path = f"{table_path}/{script_table}/data"

spark.sql(f"CREATE DATABASE if not EXISTS {backup_schema}")

table_name = f"{backup_schema}.{script_table}"

# COMMAND ----------

# MAGIC %md ### Function Imports

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import col, max, last,to_date, broadcast, length, lit, row_number, when, lead, to_timestamp, to_utc_timestamp, concat,expr, regexp_extract, substring_index, regexp_replace
import datetime
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.types import NullType

# COMMAND ----------

# MAGIC %md ### Utility Functions

# COMMAND ----------

# DBTITLE 1,Function to get Table script
def create_stmt(Query: str) -> str:
    z = ""
    create_query = str(Query).replace("'","").replace("[","").replace("]","")
    try:
        show_query = spark.sql(create_query)
        x = show_query.select('createtab_stmt').collect()[0][0]
        y = x.replace('silver', 'bronze')
        return y
    
    except Exception as e:
        print(create_query)
        print("Its a view.")

# COMMAND ----------

# MAGIC %md ### Read database from Hivemetastore

# COMMAND ----------

# DBTITLE 1,Read database which are included
database_included = spark.sql("show databases like ''")

if isinstance(silver_database, str):
    database = silver_database.replace(" ", "").split(",")
else:
    database = silver_database

for i in database:
    db_query = f"show databases like '{i}'"
    df= spark.sql(db_query)
    database_included = database_included.union(df)
display(database_included)

# COMMAND ----------

# DBTITLE 1,Read database tables 
table_df = spark.sql("show tables in default like ''")
for db in database_included.collect():
    df = spark.sql(f"show tables in {db.databaseName}")
    table_df = table_df.union(df)

display(table_df)

# COMMAND ----------

# DBTITLE 1,Create tables
table_df=table_df.withColumn("Query",concat(lit("show create table "),col("database"),lit("."),concat("tableName")))
table_drop=table_df.withColumn("DropQuery",concat(lit("drop table "),col("database"),lit("."),concat("tableName")))


panas_df=table_drop.toPandas()
panas_df['stmt']=panas_df['Query'].apply(lambda x: create_stmt(x))

display(panas_df)

final_df = spark.createDataFrame(panas_df)
# display(final_df)
final_df.write.format("delta").mode("overwrite").saveAsTable(table_name, path = table_path )
