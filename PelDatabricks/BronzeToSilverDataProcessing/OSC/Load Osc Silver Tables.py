# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
import os
import json
from delta.tables import DeltaTable
from ast import literal_eval
import os.path
from os import path
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window
from datetime import timedelta
import pandas as pd
import datetime

# COMMAND ----------

dbutils.widgets.text("ZOrderColumnList", "")
dbutils.widgets.text("SourceLandingZonePath", "")
dbutils.widgets.text("SourceServiceName", "")
dbutils.widgets.text("SourceEntityName", "")
dbutils.widgets.text("CurrentDate", "")
dbutils.widgets.text("DateSet", "")
dbutils.widgets.text("SourceExtractMethod", "")
dbutils.widgets.text("KeyColumnName", "")
dbutils.widgets.text("UnityCatalogName", "")
dbutils.widgets.text("StorageAccountName", "")
dbutils.widgets.text("LandingDirName", "")
dbutils.widgets.text("BronzeDirName", "")

# COMMAND ----------

zorder_column_list_value = dbutils.widgets.get("ZOrderColumnList")
source_landing_zone_path = dbutils.widgets.get("SourceLandingZonePath")
source_entity_name = dbutils.widgets.get("SourceEntityName")
source_service_name = dbutils.widgets.get("SourceServiceName")
date_param = dbutils.widgets.get("DateSet")
current_date = dbutils.widgets.get("CurrentDate")
source_extract_method = dbutils.widgets.get("SourceExtractMethod")
key_column_name = dbutils.widgets.get("KeyColumnName")
unity_catalog_name = dbutils.widgets.get("UnityCatalogName")
storage_account_name = dbutils.widgets.get("StorageAccountName")
landing_dir = dbutils.widgets.get("LandingDirName")
bronze_dir = dbutils.widgets.get("BronzeDirName")

container_name = "master"
uc_schema = "pel_osc"

abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

source_landing_zone_path = landing_dir + source_landing_zone_path
source_landing_zone_path = abfss_path + source_landing_zone_path

# COMMAND ----------

create_db_ddl = f"""create database if not exists {unity_catalog_name}.{uc_schema}"""
spark.sql(create_db_ddl)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

bronze_delta_table_path = (
    abfss_path + bronze_dir + source_service_name.upper() + "/" + source_entity_name
)
bronze_table_name = source_service_name + "_" + source_entity_name

# COMMAND ----------

print(
    "bronze_delta_table_path:",
    bronze_delta_table_path,
    "\n\nbronze_table_name:",
    bronze_table_name,
    "\n\nsource_landing_zone_path:",
    source_landing_zone_path,
)

# COMMAND ----------

updates_df = (
    spark.read.format("parquet").load(source_landing_zone_path).dropDuplicates()
)

# COMMAND ----------

updates_df.count()

# COMMAND ----------

#Remove dupilcate records due to delta
if("Report" in source_landing_zone_path and (source_extract_method=='DELTA')):
    if f"{date_param}" in updates_df.columns:
        updates_df = updates_df.withColumn("rank", rank().over(Window.partitionBy(*key_column_name.split(",")).orderBy(col(f"{date_param}").desc()))).where("rank = 1").drop("rank")
    else:
        print("date_param not valid")

# COMMAND ----------

updates_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Osc Table

# COMMAND ----------

## FULL LOAD (DAILY / INCREMENT)
if source_extract_method == "FULL" and DeltaTable.isDeltaTable(
    spark, bronze_delta_table_path
):
    print("Load Type : FULL, Table is present, Table Path is Present")
    spark.sql(
        "Truncate Table "
        + unity_catalog_name
        + "."
        + uc_schema
        + "."
        + bronze_table_name
    )
    print("Trucate Table")
    updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
    print("Insert Data")
elif source_extract_method == "FULL":
    print("Load Type : FULL ,bronze Table Not present,bronze Table Path Not Present")
    updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
    print("Files saved")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
        + unity_catalog_name
        + "."
        + uc_schema
        + "."
        + bronze_table_name
        + ' using DELTA LOCATION "'
        + bronze_delta_table_path
        + '" '
    )
    print("Table created")
## DELTA LOAD (Increment)
elif source_extract_method == "DELTA":
    print("Load Type : DELTA, Table is present, Table Path is Present")
    key_column_output = ""
    key_column_list = key_column_name.split(",")
    count = 0
    for i in key_column_list:
        key_column_output += (
            f"t.{i} = s.{i} {'and' if count < len(key_column_list)-1 else ''} "
        )
        count = count + 1

    # Read the existing Delta table into a DataFrame
    delta_table = DeltaTable.forPath(spark, bronze_delta_table_path)

    # Merge the parquet DataFrame into the Delta table using `merge`
    # and `whenMatchedUpdateAll`

    if len(key_column_list) > 1:
        print("Multiple Primary Keys")
        delta_table.alias("t").merge(
            updates_df.alias("s"), f"{key_column_output}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        print("single Primary Keys")
        delta_table.alias("t").merge(
            updates_df.alias("s"), f"t.{key_column_name} = s.{key_column_name}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

##Append WIth Creating a new increment Key

elif source_extract_method == "APPEND" and DeltaTable.isDeltaTable(
    spark, bronze_delta_table_path
):
    print("Load Type : APPEND ,bronze Table present,bronze Table Path Present")
    bronze_df = spark.read.format("delta").load(bronze_delta_table_path)
    w = Window().orderBy(updates_df.columns)
    updates_df = updates_df.withColumn(
        "dl_index", row_number().over(w) + bronze_df.count()
    )
    updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
elif source_extract_method == "APPEND":
    print("Load Type : APPEND ,bronze Table Not present,bronze Table Path Not Present")
    w = Window().orderBy(updates_df.columns)
    updates_df = updates_df.withColumn("dl_index", row_number().over(w))
    updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
        + unity_catalog_name
        + "."
        + uc_schema
        + "."
        + bronze_table_name
        + ' using DELTA LOCATION "'
        + bronze_delta_table_path
        + '" '
    )

##Append WIthout Creating a new increment Key

elif source_extract_method == "DATAAPPEND" and DeltaTable.isDeltaTable(
    spark, bronze_delta_table_path
):
    print("Load Type : VALUEAPPEND ,bronze Table present,bronze Table Path Present")
    updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
elif source_extract_method == "DATAAPPEND":
    print(
        "Load Type : VALUEAPPEND ,bronze Table Not present,bronze Table Path Not Present"
    )
    updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
        + unity_catalog_name
        + "."
        + uc_schema
        + "."
        + bronze_table_name
        + ' using DELTA LOCATION "'
        + bronze_delta_table_path
        + '" '
    )

# COMMAND ----------

# Setup Value For next control table
if "Report" in source_landing_zone_path and (source_extract_method == "APPEND"):
    if date_param in updates_df.columns:
        if updates_df.count() != 0:
            next_initial_value = updates_df.select(max(f"{date_param}")).collect()[0][0]
            next_initial_value = pd.to_datetime(next_initial_value) + timedelta(seconds=1)
            datetime_obj = str(next_initial_value)
            datetime_obj = pd.to_datetime(datetime_obj)
            formatted_datetime_str = (
                datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            )
            dbutils.notebook.exit(formatted_datetime_str)
        if updates_df.count() == 0:
            next_initial_value = spark.read.format('delta').load(bronze_delta_table_path).select(max(f"{date_param}")).collect()[0][0]
            next_initial_value = pd.to_datetime(next_initial_value) + timedelta(seconds=1)
            datetime_obj = str(next_initial_value)
            datetime_obj = pd.to_datetime(datetime_obj)
            formatted_datetime_str = (
                datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            )
            dbutils.notebook.exit(formatted_datetime_str)
        
