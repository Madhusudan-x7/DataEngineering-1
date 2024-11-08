# Databricks notebook source
# MAGIC %md # Notebook Set Up

# COMMAND ----------

spark.conf.set("spark.databricks.delta.vacuum.logging.enabled",True)

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

# DBTITLE 1,Get Set Parmeters
dbutils.widgets.text("ExceptDatabase","")
dbutils.widgets.text("IncludeDatabase","")
dbutils.widgets.text("RetentionPeriod","")
dbutils.widgets.text("FilePath","")

except_database = dbutils.widgets.get("ExceptDatabase")
include_database = dbutils.widgets.get("IncludeDatabase")
retention_period = dbutils.widgets.get("RetentionPeriod")
file_path = dbutils.widgets.get("FilePath")

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

# DBTITLE 1,Function to get Location and Retention Period
def table_location(Query: str) -> str:
    z = ""
    create_query = str(Query).replace("'","").replace("[","").replace("]","")
    try:
        show_query = spark.sql(create_query)
        x = show_query.withColumn("retention", F.to_json("properties"))\
               .select("location",
                      "lastModified",
                      "sizeInBytes",
                      "retention"
                     )

        y =  x.withColumn("retention_duration", when(col("retention").contains('deletedFileRetentionDuration'), col("retention")).otherwise(lit("{}")))\
              .withColumn("last_Modified", col("lastModified").cast("String"))\
               .select("location",
                       "sizeInBytes",
                       "last_Modified",
                       "retention_duration"
                      )

        z = str(y.collect())
        return z
    
    except Exception as e:
        print(create_query)
        print("Its a view. DESCRIBE DETAIL is only supported for tables.")

# COMMAND ----------

# DBTITLE 1,Function to Vacuum Tables
def table_vacuum(Query: str) -> str:
    create_query = str(Query).replace("'","").replace("[","").replace("]","")
    vac_query = ""
    try:
        vac_query = spark.sql(create_query)
        y = str(vac_query.collect())
    except Exception as e:
        print(create_query)
        y = "Table exists in Hivemetastore, but path does not exists in ADLS"
        print(y)
    return y

# COMMAND ----------

# MAGIC %md ### Read database from Hivemetastore

# COMMAND ----------

# DBTITLE 1,Read database which are excluded
database_excluded = spark.sql("show databases like ''")
database = except_database.split(',')
for i in database:
    db_query = f"show databases like '{i}'"
    df= spark.sql(db_query)
    database_excluded = database_excluded.union(df)
display(database_excluded)

# COMMAND ----------

# DBTITLE 1,Read database which are included
database_included = spark.sql("show databases like ''")
database = include_database.split(',')
for i in database:
    db_query = f"show databases like '{i}'"
    df= spark.sql(db_query)
    database_included = database_included.union(df)
display(database_included)

# COMMAND ----------

# DBTITLE 1,Generate list of Database to vaccum
print(database_excluded.count())

if database_excluded.count() > 0:
    db_query = f"show databases"
    all_database= spark.sql(db_query)
    vacuum_database = all_database.subtract(database_excluded)
else:
    vacuum_database = database_included
display(vacuum_database)

# COMMAND ----------

# DBTITLE 1,Read database tables to Vacuum
table_df = spark.sql("show tables in default like ''")
for db in vacuum_database.collect():
    df = spark.sql(f"show tables in {db.databaseName}")
    table_df = table_df.union(df)

#table_df = table_df.filter(col('tableName') != lit('project')).filter(col('tableName') != lit('lineitemall_quarantine'))
display(table_df)

# COMMAND ----------

# MAGIC %md ### Generate Audit File and Vacuum table 

# COMMAND ----------

# DBTITLE 1,Read database tables and Vacuum (Pandas)
table_df=table_df.withColumn("Query",concat(lit("DESC DETAIL "),col("database"),lit("."),concat("tableName")))
panas_df=table_df.toPandas()

panas_df['stmt']=panas_df['Query'].apply(lambda x: table_location(x))

pandas_df =  panas_df[panas_df['stmt'].notnull()]
pandas_df["location"] = pandas_df.stmt.apply(lambda x: x.split(",")[0]).str[20:]
pandas_df["sizeInBytes"] = pandas_df.stmt.apply(lambda x: x.split(",")[1])
pandas_df["lastmodified"] = pandas_df.stmt.apply(lambda x: x.split(",")[2])
pandas_df["duration_temp"] = pandas_df.stmt.apply(lambda x: x.split("{")[1])
pandas_df["duration_temp_size"] = pandas_df.stmt.apply(lambda x: len(x.split("{")[1]))

#col = 'duration_temp_size'
pandas_df["duration"] = pandas_df.stmt.apply(lambda x: None if len(x.split("=")[4].split("interval")) == 1 else x.split("=")[4].split("interval")[1].split(" ")[1])

#pandas_df["duration"]  = np.where(pandas_df[col] > 81, pandas_df['duration_temp'].str.slice(83, 86), None)

pandas_df["retention_duration"] = pandas_df.duration.apply(lambda x: (int(x)*24) if x != None else None)

#pandas_df["vacuum_query"] = 'VACUUM '+pandas_df['database'].map(str) +'.'+pandas_df['tableName']+' RETAIN '+retention_period+ ' HOURS'

pandas_df["vacuum_query"] = np.where((pandas_df["retention_duration"] < float(retention_period)), 'VACUUM '+pandas_df['database'].map(str) +'.'+pandas_df['tableName']+' RETAIN '+pandas_df['retention_duration'].map(str) + ' HOURS','VACUUM '+pandas_df['database'].map(str) +'.'+pandas_df['tableName']+' RETAIN '+retention_period+ ' HOURS')


pandas_df['status'] = np.where((((pandas_df["location"] != "") & (pandas_df["location"].str.contains("/mnt/master"))) & ((pandas_df["retention_duration"] <= int(retention_period)) | pandas_df["retention_duration"].isnull())) , "Vacuumable", "Not Vacuumable")

non_vacuum_df = pandas_df[['database', 'tableName','location','sizeInBytes','lastmodified','retention_duration','vacuum_query','status']]
non_vacuum_df = non_vacuum_df[non_vacuum_df["status"] == "Not Vacuumable"]

vacuum_df = pandas_df[pandas_df["status"]== "Vacuumable"]
vacuum_df = vacuum_df[['database', 'tableName','location','sizeInBytes','lastmodified','retention_duration','vacuum_query','status']]

vacuum_df["status"] = vacuum_df["vacuum_query"].apply(lambda x: table_vacuum(x))
vacuum_df["status"] = vacuum_df["status"].apply(lambda x: "Vacuum successful" if ('Row') in x else "Not Vacuumable")

final_df= pd.concat([vacuum_df, non_vacuum_df]).drop_duplicates()
final_df = spark.createDataFrame(final_df)

display(final_df)

# COMMAND ----------

# MAGIC %md ### Write data to Audit file

# COMMAND ----------

# DBTITLE 1,Write Vacuum table list for Audit
from pyspark.sql.functions import *

ins_gmts=datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

file_path = file_path + "/" + ins_gmts

df_final = final_df.select([
    F.lit(None).cast('string').alias(i.name)
    if isinstance(i.dataType, NullType)
    else i.name
    for i in final_df.schema
])

df_write = df_final.withColumn("size_Bytes", expr("substring(sizeInBytes, 14, 20)"))\
                   .withColumn("last_modified", expr("substring(lastmodified, 17, 19)"))\
                   .select("database",
                           "tableName",
                           "location",
                           "retention_duration",
                           "size_Bytes",
                           "last_modified",
                           "status")

df_write.coalesce(1)\
     .write.format("csv")\
     .option("header",True)\
     .mode("overwrite")\
     .save(file_path, nullValue=None)
