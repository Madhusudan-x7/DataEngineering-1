# Databricks notebook source
# MAGIC %md # Salesforce Silver Layer Notebook

# COMMAND ----------

# MAGIC %md ## Importing Libraries

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
import os
import json
from datetime import datetime

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

# MAGIC %md ##Defining Widgets

# COMMAND ----------

dbutils.widgets.text("SourceKeyColumnList","")
dbutils.widgets.text("SourceObjectPath","")
dbutils.widgets.text("SourceObjectName","")
dbutils.widgets.text("SourceSystemName","")
dbutils.widgets.text("SourceExtractMethod","")
dbutils.widgets.text("DeltaUpdateWatermarkColumnName","")

dbutils.widgets.text("UnityCatalogName","")
dbutils.widgets.text("DataLakeDestinationContainer","")
dbutils.widgets.text("StorageAccountName","")
dbutils.widgets.text("BronzeFolderName","")


# COMMAND ----------

# MAGIC %md ##Loading Widgets

# COMMAND ----------

source_object_name = dbutils.widgets.get("SourceObjectName")
source_key_column_listvalue = dbutils.widgets.get("SourceKeyColumnList")
source_object_path = dbutils.widgets.get("SourceObjectPath")
source_system_name = dbutils.widgets.get("SourceSystemName")
source_extract_method = dbutils.widgets.get("SourceExtractMethod")
delta_update_watermark_column_name = dbutils.widgets.get("DeltaUpdateWatermarkColumnName")
current_date = datetime.now()

unity_catlog_name = dbutils.widgets.get("UnityCatalogName")
container_name = dbutils.widgets.get("DataLakeDestinationContainer")
storage_account_name = dbutils.widgets.get("StorageAccountName")
bronze_folder_name = dbutils.widgets.get("BronzeFolderName")

uc_schema = "pel_" + source_system_name.lower()
abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

create_schema_sql = f"Create schema IF NOT EXISTS {unity_catlog_name}.{uc_schema}"
spark.sql(create_schema_sql)

bronze_delta_table_path = abfss_path + bronze_folder_name + "/"  + source_system_name.lower() + '/' + source_object_name + '/' 
bronze_table_name = unity_catlog_name + "." + uc_schema + '.' + source_object_name

# COMMAND ----------

updates_df = spark.read.parquet(abfss_path + source_object_path)

# COMMAND ----------

source_key_column_list = source_key_column_listvalue.split(',')
len_source_key_column_list = len(source_key_column_list)
join_condition =''

for count,key in enumerate(source_key_column_list):
    if len_source_key_column_list != 1:
        join_condition = join_condition+ 'original.' + key +'= updates.' + key + ' AND '
        len_source_key_column_list = len_source_key_column_list - 1
    else:
        join_condition = join_condition+ 'original.' + key +'= updates.' + key + ' '

# COMMAND ----------

if (
    source_extract_method == "FULL" and DeltaTable.isDeltaTable(spark, bronze_delta_table_path)
):
    spark.sql("Truncate Table " + bronze_table_name)
    updates_df = updates_df.drop_duplicates(source_key_column_list) 
    updates_df = updates_df.selectExpr(["*"]).withColumn("BronzeLastModifiedDate", date_format(lit(current_date.strftime("%Y-%m-%d %H:%M:%S")), "yyyyMM"))
    updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
    print("FULL Load: truncated the table and insert the complete data")
    print(updates_df.count())
    
elif DeltaTable.isDeltaTable(spark, bronze_delta_table_path): 
     print(join_condition)
     print("Delta Load ")
     print(updates_df.count())
     updates_df = updates_df.dropDuplicates(source_key_column_list)
     print("After Removing Duplicates")
     print(updates_df.count())
     updates_df = updates_df.selectExpr(["*"]).withColumn("BronzeLastModifiedDate", date_format(lit(current_date.strftime("%Y-%m-%d %H:%M:%S")), "yyyyMM"))
     delta_table = DeltaTable.forPath(spark, bronze_delta_table_path)
     delta_table.alias("original").merge(
        updates_df.alias("updates"),
         join_condition)\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
    if source_extract_method == "FULL":
        updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
        spark.sql("CREATE TABLE IF NOT EXISTS " + bronze_table_name + ' using DELTA LOCATION "' + bronze_delta_table_path + '" ')
    else:
        print("Regular tables load Rest conditions")
        updates_df.selectExpr(["*"] + [f"{delta_update_watermark_column_name} as BronzeLastModifiedDate"]).withColumn("BronzeLastModifiedDate", date_format("BronzeLastModifiedDate", "yyyyMM"))\
        .write.partitionBy("BronzeLastModifiedDate").format("delta").mode("append").save(bronze_delta_table_path)
        print("Create and load tables")
        spark.sql("CREATE TABLE IF NOT EXISTS " + bronze_table_name + ' using DELTA LOCATION"' + bronze_delta_table_path + '" ')
        spark.sql("OPTIMIZE " + bronze_table_name + " ZORDER BY (" + source_key_column_listvalue + ")" )

# COMMAND ----------

print(f" Bronze Load Finished For \n bronze_table_name :\t\t {bronze_table_name}  \n bronze_delta_table_path :\t {bronze_delta_table_path}")
