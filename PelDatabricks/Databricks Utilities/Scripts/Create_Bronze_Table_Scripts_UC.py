# Databricks notebook source
# MAGIC %md # Notebook Set Up

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

# DBTITLE 1,Get Set Parmeters
dbutils.widgets.text("SilverCatalog","")

silver_catalogs = dbutils.widgets.get("SilverCatalog")

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

# DBTITLE 1,Function to create Bronze Table
def create_table(Query: str) -> str:
    z = ""
    create_tble = str(Query).replace("'","").replace("[","").replace("]","")
    try:
        show_query = spark.sql(create_tble)
        x = "Table created Successfully"
        return x
    
    except Exception as e:
        x = "Table not created"
        return x

# COMMAND ----------

# DBTITLE 1,Read database which are included
catalogs_included = spark.sql("SHOW CATALOGS like ''")

if isinstance(silver_catalogs, str):
    catalogs = silver_catalogs.replace(" ", "").split(",")
else:
    catalogs = silver_catalogs
    
#catalogs = include_catalogs.split(',')
for i in catalogs:
    ct_query = f"show catalogs like '{i}'"
    df= spark.sql(ct_query)
    catalogs_included = catalogs_included.union(df)
display(catalogs_included)

# COMMAND ----------

# DBTITLE 1,Read database tables 
table_df = spark.sql("Select table_catalog,table_schema,table_name from main.information_schema.tables")

for db in catalogs_included.collect():
    df = spark.sql(f"Select table_catalog,table_schema,table_name from {db.catalog}.information_schema.tables where table_type = 'EXTERNAL'")
    table_df = table_df.union(df)

table_df_final = table_df.filter(col('table_schema') != lit('information_schema'))
display(table_df_final)

# COMMAND ----------

# DBTITLE 1,Create tables
table_df=table_df_final.withColumn("Query",concat(lit("Show create table "),col("table_catalog"),lit("."),concat("table_schema"),lit("."),concat("table_name")))
panas_df=table_df.toPandas()

panas_df['stmt']=panas_df['Query'].apply(lambda x: create_stmt(x))
panas_df['status']=panas_df['stmt'].apply(lambda x: create_table(x))
display(panas_df)
