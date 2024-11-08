# Databricks notebook source
# MAGIC %md # Genric bronze Layer Notebook

# COMMAND ----------

# MAGIC %md ## Importing Libraries

# COMMAND ----------

# DBTITLE 1,Libraries
from delta.tables import *
from pyspark.sql.functions import *
import os
import json

# COMMAND ----------

# MAGIC %md ## Set Configs

# COMMAND ----------

# DBTITLE 1,Configs
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

# MAGIC %md ##Defining Widgets

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("SourceKeyColumnList","")
dbutils.widgets.text("SourceTablePath","")
dbutils.widgets.text("SourceTableName","")
dbutils.widgets.text("SourceServerName","")
dbutils.widgets.text("ServerFolderName","")
dbutils.widgets.text("SourceDatabaseName","")
dbutils.widgets.text("DBFolderName","")
dbutils.widgets.text("SourceSchemaName","")
dbutils.widgets.text("SourceExtractMethod","")
dbutils.widgets.text("DeltaUpdateWatermarkColumnName","")
dbutils.widgets.text("SCDType","")
dbutils.widgets.text("BronzeTableAppend","")
dbutils.widgets.text("BronzeTableDeleteCondition","")

dbutils.widgets.text("UnityCatalogName","")
dbutils.widgets.text("UCFlag","")
dbutils.widgets.text("UCSchemaPrefix","")
dbutils.widgets.text("DataLakeDestinationContainer","")
dbutils.widgets.text("StorageAccountName","")
dbutils.widgets.text("TableSchemaName","")

# COMMAND ----------

# MAGIC %md ##Loading Widgets

# COMMAND ----------

# DBTITLE 1,setting Input variable values
source_key_column_listvalue = dbutils.widgets.get("SourceKeyColumnList")
table_schema = dbutils.widgets.get("TableSchemaName")
source_table_name = dbutils.widgets.get("SourceTableName")
server_folder_name = dbutils.widgets.get("ServerFolderName")
source_server_name = dbutils.widgets.get("SourceServerName")
source_database_name = dbutils.widgets.get("SourceDatabaseName")
db_folder_name = dbutils.widgets.get("DBFolderName")
source_schema_name = dbutils.widgets.get("SourceSchemaName")
source_extract_method = dbutils.widgets.get("SourceExtractMethod").upper()
delta_update_watermark_column_name = dbutils.widgets.get("DeltaUpdateWatermarkColumnName")
scd_type = dbutils.widgets.get("SCDType")
bronze_table_append=dbutils.widgets.get("BronzeTableAppend")
bronze_table_delete_condition=dbutils.widgets.get("BronzeTableDeleteCondition")

unity_catlog_name = dbutils.widgets.get("UnityCatalogName")
uc_flag=dbutils.widgets.get("UCFlag")
uc_schema_prefix=dbutils.widgets.get("UCSchemaPrefix")

container_name = dbutils.widgets.get("DataLakeDestinationContainer")
storage_account_name = dbutils.widgets.get("StorageAccountName")
table_schema_name = dbutils.widgets.get("TableSchemaName")

abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
source_table_path = abfss_path +dbutils.widgets.get("SourceTablePath")

# COMMAND ----------

# DBTITLE 1,Setting variables
table_schema = 'bronze'

if server_folder_name == "":
    server_folder_name = source_server_name
    
if db_folder_name == "":
    db_folder_name = source_database_name

if source_key_column_listvalue.upper() == "NA":
    source_key_column_listvalue = ""

if uc_schema_prefix == "":
    uc_schema = source_database_name.lower()
else:
    uc_schema = uc_schema_prefix.lower() + "_" + source_database_name.lower()

if bronze_table_delete_condition == "":
    bronze_table_delete_condition = "NA"

abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

# DBTITLE 1,Setting up Bronze table name and Bronze table path
create_schema_sql = f"Create schema IF NOT EXISTS {unity_catlog_name}.{uc_schema}"
spark.sql(create_schema_sql)
if scd_type == '2':
    bronze_delta_table_path = abfss_path + "" + table_schema + "/" + server_folder_name+'/'+db_folder_name+'/' + source_schema_name + '/' + source_table_name + '_type2/' 
    bronze_table_name = unity_catlog_name + "." + uc_schema + "." +   source_schema_name + '_' + source_table_name + '_type2'
else:
    bronze_delta_table_path = abfss_path + "" + table_schema + "/" + server_folder_name+'/'+db_folder_name+'/' + source_schema_name + '/' + source_table_name + '/' 
    bronze_table_name = unity_catlog_name + "." + uc_schema + "." + source_schema_name + '_' + source_table_name

# COMMAND ----------

# DBTITLE 1,Reading bronze layer data and creating List of key columns and non-key columns
updates_df= spark.read.parquet(source_table_path + '*.parquet')

list_of_source_key_column = list(source_key_column_listvalue.split(","))

non_key_column = []

for field in updates_df.schema.fields:
  exist_count = list_of_source_key_column.count(field.name)
  if (exist_count == 0 ):
    non_key_column.append(field.name)

source_column_list_value = ','.join(map(str, non_key_column))

# COMMAND ----------

# DBTITLE 1,Creating Join Condition to merge table.
if len(source_key_column_listvalue)>0:
    source_key_column_list = source_key_column_listvalue.split(',')
    len_source_key_column_list = len(source_key_column_list)
    join_condition =''
    join_condition2 =''
    merge_condition =''
    merge_condition2 = ''
    merge_condition3 = ''
    insert_condition1 = ''
    for count,key in enumerate(source_key_column_list):
        if len_source_key_column_list != 1:
            join_condition = join_condition+ 'original.' + key +'= updates.' + key + ' AND '
            join_condition2 = join_condition2+ ''"'" + key + "'"', '
            merge_condition = merge_condition+ 'NULL as mergeKey' + str(count+1) + ', '
            merge_condition2 = merge_condition2+ ''+ key +'' ' as mergeKey' + str(count+1) + ', '
            merge_condition3 = merge_condition3+'original.'+ key +'' ' = mergeKey' + str(count+1) + ' AND '
            insert_condition1 = insert_condition1+'"'+ key +'"' ' :"staged_updates.' + key + '" , '
            len_source_key_column_list = len_source_key_column_list - 1
        else:
            join_condition = join_condition+ 'original.' + key +'= updates.' + key + ' '
            join_condition2 = join_condition2+ ''"'" + key +"'"''
            merge_condition = merge_condition+ 'NULL as mergeKey' + str(count+1) + ' ' 
            merge_condition2 = merge_condition2+ ''+ key +'' ' as mergeKey' + str(count+1) + ' ' 
            merge_condition3 = merge_condition3+'original.'+ key +'' ' = mergeKey' + str(count+1) + ' ' 
            insert_condition1 = insert_condition1+'"'+ key +'"' ' :"staged_updates.' + key + '", '

    join_condition2 = '['+join_condition2+']' 
else:
    join_condition2 = []

# COMMAND ----------

# DBTITLE 1,Creating Update Condition and Insert Condition for scd2 Load.
if scd_type == '2':
    source_column_list = source_column_list_value.split(',')
    len_source_column_list = len(source_column_list)
    update_condition =''
    update_condition2=''
    insert_condition2=''
    for key in source_column_list:
        if len_source_column_list != 1:
            update_condition = update_condition+ 'original.' + key +'<> updates.' + key + ' OR '
            update_condition2 = update_condition2+ 'original.' + key +'<> staged_updates.' + key + ' OR '
            insert_condition2 = insert_condition2+'"'+ key +'"' ' :"staged_updates.' + key + '", '
            len_source_column_list = len_source_column_list -1
        else:
            update_condition = update_condition+ 'original.' + key +'<> updates.' + key + ' '
            update_condition2 = update_condition2+ 'original.' + key +'<> staged_updates.' + key + ' '
            insert_condition2 = insert_condition2+'"'+ key +'"' ' :"staged_updates.' + key + '","File_Date": "staged_updates.File_Date" ,"DL_ARRIVAL_DTT": "staged_updates.DL_ARRIVAL_DTT" ,"DL_UPDATE_DTT":"staged_updates.DL_UPDATE_DTT" ,"Flag": "1" '

# COMMAND ----------

# DBTITLE 1,Load Bronze Table
if DeltaTable.isDeltaTable(spark, bronze_delta_table_path):
    print("Bronze Table Exists")
    # Table exists 
    if bronze_table_delete_condition.upper() != "NA":
        # Delete condition present
        print("Delete condition is present, Need to Delete and Load the data")
        print(f"DELETE FROM {bronze_table_name} WHERE {bronze_table_delete_condition}")
        spark.sql(f"DELETE FROM {bronze_table_name} WHERE {bronze_table_delete_condition}")
        print("Delete Completed")   
    else:
        print("Delete condition is not present, No Need to Delete the data")
    
    # Check extract method 
    print("Checking Source Extract Method")
    if source_extract_method == "FULL":
        # Full Load (Truncate and Load)
        print("Source Extract Method: Full Load - Need to Truncating Table and Reload the data")
        print(f"Truncate Table {bronze_table_name}")
        spark.sql("Truncate Table " + bronze_table_name)
        print("Truncate Completed")
        updates_df = updates_df.selectExpr(["*"])\
                .withColumn("File_Date", current_timestamp().cast(StringType()))\
                .withColumn("File_Date", date_format("File_Date", "yyyyMM"))
        updates_df.write.format("delta").mode("append").option("path", f"{bronze_delta_table_path}").saveAsTable(f"{bronze_table_name}")

    elif ((source_extract_method == "DELTA") or (source_extract_method == "SQL") or (source_extract_method == "DELTAID")):
        # Source Extract Method is Delta/SQL
        print("Source Extract Method: Delta/SQL")
        if len(source_key_column_listvalue)==0 :
            # No Source Key Append Only
            print("Append Only without having Primary Key")
            updates_df = updates_df.selectExpr(["*"])\
                .withColumn("File_Date", when(expr(f"CAST({delta_update_watermark_column_name} AS BIGINT) IS NOT NULL"),  current_timestamp().cast(StringType())).otherwise(col(f"{delta_update_watermark_column_name}")))\
                .withColumn("File_Date", date_format("File_Date", "yyyyMM"))
            updates_df.write.format("delta").mode("append").save(bronze_delta_table_path)

        elif ((len(source_key_column_listvalue)>0) and scd_type == "1"):
            # SCD Type 1
            print("Source Extract Method: SCD Type 1")
            updates_df = updates_df.selectExpr(["*"])\
                        .withColumn("File_Date", when(expr(f"CAST({delta_update_watermark_column_name} AS BIGINT) IS NOT NULL"),  current_timestamp().cast(StringType())).otherwise(col(f"{delta_update_watermark_column_name}")))\
                        .withColumn("File_Date", date_format("File_Date", "yyyyMM"))
            delta_table = DeltaTable.forPath(spark, bronze_delta_table_path)
            delta_table.alias("original").merge(updates_df.alias("updates"), join_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        elif ((len(source_key_column_listvalue)>0) and scd_type == "2"):
            # SCD Type 2
            print("Source Extract Method: SCD Type 2")
            updates_df = updates_df.selectExpr(["*"]).withColumn("File_Date", when(expr(f"CAST({delta_update_watermark_column_name} AS BIGINT) IS NOT NULL"),  current_timestamp().cast(StringType())).otherwise(col(f"{delta_update_watermark_column_name}"))).withColumn("File_Date", date_format("File_Date", "yyyyMM")).withColumn("DL_ARRIVAL_DTT", current_timestamp()).withColumn("DL_UPDATE_DTT", current_timestamp()).withColumn("Flag", lit("1"))

            delta_table = DeltaTable.forPath(spark, bronze_delta_table_path)

            # Rows to INSERT new records.
            new_record_to_insert = (updates_df.alias("updates").join(delta_table.toDF().alias("original"), eval(join_condition2)).where("original.Flag = 1 AND " + update_condition).filter(delta_table.toDF().Flag != "0"))

            # Stage the update by unioning two sets of rows
            # 1. Rows that will be inserted in the whenNotMatched clause
            # 2. Rows that will either update the current records in existing records or insert the new records.

            var1 = "" + merge_condition + "," "updates.*" ""
            column_expr1 = var1.replace("'", "").split(",")
            column_expr1 = [x.strip(" ") for x in column_expr1]
            var2 = "" + merge_condition2 + ",'" "*" "'"
            column_expr2 = var2.replace("'", "").split(",")

            updates_df.alias("updates")

            new_record_to_insert = new_record_to_insert.selectExpr(column_expr1)
            new_record_to_insert = new_record_to_insert.select(sorted(new_record_to_insert.columns))

            updates_df2 = updates_df.selectExpr(column_expr2)
            updates_df2 = updates_df2.select(sorted(updates_df2.columns))

            staged_updates = new_record_to_insert.union(updates_df2)
            staged_updates = staged_updates.distinct()

            # Preparing Parameters for Merge Opertion
            insert_condition = insert_condition1 + insert_condition2
            insert_condition = "'{" + insert_condition + "}'"

            test_string = strtest_string = eval(insert_condition)
            res = json.loads(test_string)  # Dictionary Creation of Values to be Inserted.

            # Apply SCD Type 2 operation using merge
            delta_table.alias("original").merge(
                staged_updates.alias("staged_updates"), "" + merge_condition3 + ""
                ).whenMatchedUpdate(
                    condition="original.Flag = 1 AND (" + update_condition2 + ")",
                    set={
                        "Flag": "0",
                        "DL_UPDATE_DTT": "staged_updates.DL_ARRIVAL_DTT",
                        },
                    ).whenNotMatchedInsert(
                        values=res
                    ).execute()
        else:
            print(f"Invalid SCD Type: {scd_type}, please check control table entry")
    else:
            print(f"Invalid Source Extract Method: {source_extract_method} and SCD Type: {scd_type}, please check control table entry")
else:
    #Table does not exists
    print("Table not present, creating new table")
    updates_df = updates_df.selectExpr(["*"])\
                .withColumn("File_Date", current_timestamp().cast(StringType()))\
                .withColumn("File_Date", date_format("File_Date", "yyyyMM"))
    updates_df.write.format("delta").mode("overwrite").option(
        "path", f"{bronze_delta_table_path}"
        ).saveAsTable(f"{bronze_table_name}")
    print("Data loaded successfully")

# COMMAND ----------

print(f" {table_schema} Load Finished For \n {table_schema}_table_name :\t\t {bronze_table_name}  \n {table_schema}_delta_table_path :\t {bronze_delta_table_path}")
