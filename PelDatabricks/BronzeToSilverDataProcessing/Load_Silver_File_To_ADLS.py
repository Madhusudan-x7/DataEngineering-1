# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql.functions import col, lit, substring, substring_index, split
from delta.tables import DeltaTable
import os
from datetime import datetime
import json
from pyspark.sql.functions import *
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Set Parameters
dbutils.widgets.text("BronzePath","")
dbutils.widgets.text("SourceFileFormat","")
dbutils.widgets.text("ColList","")
dbutils.widgets.text("FilterColumn","")
dbutils.widgets.text("FixColumn","")
dbutils.widgets.text("Dic","")
dbutils.widgets.text("DataAction","")
dbutils.widgets.text("FileDelimiter","")
dbutils.widgets.text("Header","")

dbutils.widgets.text("UnityCatalogName","")
dbutils.widgets.text("UCFlag","")
dbutils.widgets.text("UCSchemaPrefix","")
dbutils.widgets.text("BronzeDestinationContainer","")
dbutils.widgets.text("StorageAccountName","")
dbutils.widgets.text("SourceStorageAccountName","")
dbutils.widgets.text("SourceType","")
dbutils.widgets.text("SourceSchemaName","")
dbutils.widgets.text("SourceTableName","")
dbutils.widgets.text("DataLakeDestinationContainer","")
dbutils.widgets.text("DataLakeDestinationFolder","")
dbutils.widgets.text("BronzeSchema","")

# COMMAND ----------

# DBTITLE 1,Get Parameters
bronze_path=dbutils.widgets.get("BronzePath")
source_fileformat=dbutils.widgets.get("SourceFileFormat")
col_list=dbutils.widgets.get("ColList")
filter_column=dbutils.widgets.get("FilterColumn")
fix_column=dbutils.widgets.get("FixColumn")
dic=dbutils.widgets.get("Dic")
data_action=dbutils.widgets.get("DataAction")
file_delimiter=dbutils.widgets.get("FileDelimiter")
header=dbutils.widgets.get("Header")

#Added Source/Destination Container/Storage Account names to help differentiate for FTPS and Unity Catalog 
destination_container_name = dbutils.widgets.get("BronzeDestinationContainer")
storage_account_name = dbutils.widgets.get("StorageAccountName")
source_storage_account_name = dbutils.widgets.get("SourceStorageAccountName")
unity_catlog_name = dbutils.widgets.get("UnityCatalogName")
uc_flag = dbutils.widgets.get("UCFlag")
uc_schema_prefix = dbutils.widgets.get("UCSchemaPrefix")
source_type = dbutils.widgets.get("SourceType")
source_schema_name = dbutils.widgets.get("SourceSchemaName")
source_table_name = dbutils.widgets.get("SourceTableName")
datalake_destination_container = dbutils.widgets.get("DataLakeDestinationContainer")
datalake_destination_folder = dbutils.widgets.get("DataLakeDestinationFolder")
bronze_schema = dbutils.widgets.get("BronzeSchema")

# COMMAND ----------

# DBTITLE 1,Set Variable
if uc_flag == "":
    uc_flag = "N"

uc_schema = ""
if uc_schema_prefix == "NULL":
    uc_schema = source_schema_name.lower()
else:
    uc_schema = uc_schema_prefix.lower() + "_" + source_schema_name.lower()

destination_abfss_path = f"abfss://{destination_container_name}@{storage_account_name}.dfs.core.windows.net"
source_abfss_path = f"abfss://{datalake_destination_container}@{source_storage_account_name}.dfs.core.windows.net"

print(uc_flag)
print(uc_schema)
print(destination_abfss_path)
print(source_abfss_path)

# COMMAND ----------

# DBTITLE 1,Path Set up
def source_path_set_up():
    source_path = ""

    if uc_flag == "N":
        if (source_type == "FTPS" or source_type == "ADLS"):
            source_path = f"/mnt/{datalake_destination_container}/{datalake_destination_folder}/{source_schema_name}/{source_table_name}/"
        elif source_type == "FTP_ADLS":
            source_path = f"/mnt/{datalake_destination_container}/{datalake_destination_folder}"
        else:
            print("Wrong Source Type, please validate")
    else:
        if (source_type == "FTPS" or source_type == "ADLS"):
            source_path = f"{source_abfss_path}/{datalake_destination_folder}/{source_schema_name}/{source_table_name}/"
        elif source_type == "FTP_ADLS":
            source_path = f"{datalake_destination_folder}"
            #change to use volumes
        else:
            print("Wrong Source Type, please validate")

    source_file_dir = source_path

    print("source_path: ", source_path)
    return source_path

def target_path_set_up():
    target_path = ""

    if uc_flag == "N":
        if (source_type == "FTPS" or source_type == "ADLS"):
            target_path = f"{bronze_path}/{source_type}/{source_schema_name}/{source_table_name}/"
        elif source_type == "FTP_ADLS":
            target_path = f"{bronze_path}/{source_type}/{source_schema_name}/{source_table_name}/"
        else:
            print("Wrong Source Type, please validate")
    else:
        if (source_type == "FTPS" or source_type == "ADLS"):
            target_path = f"{destination_abfss_path}/{bronze_schema}/{source_type}/{source_schema_name}/{source_table_name}/"
        elif source_type == "FTP_ADLS":
            target_path = f"{destination_abfss_path}/{bronze_schema}/{source_type}/{source_schema_name}/{source_table_name}/"
        else:
            print("Wrong Source Type, please validate")

    print("target_path: ", target_path)
    return target_path

def target_table_set_up():
    target_table = ""

    if uc_flag == "N":
        if (source_type == "FTPS" or source_type == "ADLS"):
            target_table = f"{bronze_schema}.{source_type}_{source_schema_name}_{source_table_name}"
        elif source_type == "FTP_ADLS":
            target_table = f"{bronze_schema}.{source_schema_name}_{source_table_name}"
        else:
            print("Wrong Source Type, please validate")
    else:
        if (source_type == "FTPS" or source_type == "ADLS"):
            target_table = f"{unity_catlog_name}.{uc_schema}.{source_table_name}"
        elif source_type == "FTP_ADLS":
            target_table = f"{unity_catlog_name}.{uc_schema}.{source_table_name}"
        else:
            print("Wrong Source Type, please validate")

    print("target_table: ", target_table)
    return target_table

source_path = source_path_set_up()
target_path = target_path_set_up()
target_table = target_table_set_up()

# COMMAND ----------

# DBTITLE 1,Create database
if uc_flag == "N":
    database=target_table.split('.')[0]
    database_query=f"Create database if not exists {database}"
    spark.sql(database_query)
    print(database)
    print(database_query)
else:
    database_schema = unity_catlog_name + "." + uc_schema
    uc_database_query=f"Create schema if not exists {database_schema}"
    spark.sql(uc_database_query)
    print(database_schema)
    print(uc_database_query)

# COMMAND ----------

# DBTITLE 1,Check if file exists or not
def file_exists():
    total=0
    extracted_source_file_name = ""
    File_name = source_schema_name.upper() +"_" + source_table_name.upper()
    for i in range(len(dbutils.fs.ls(f'{source_path}'))):
        extracted_source_file_name=dbutils.fs.ls(f'{source_path}')[i].name
        if extracted_source_file_name != 'archive/':
            File_name = extracted_source_file_name
            print(extracted_source_file_name)
            source_file_path=dbutils.fs.ls(f'{source_path}')[i].path
            print(source_file_path)
            total=total+1;
            
    if total==0:
        print("No files available for extraction")
        process_param = 'Status:{} File Not Found'.format(File_name)
        process_param = process_param.replace(":",'":"')
        output = '{{"{}"}}'.format(process_param)
        dbutils.notebook.exit(json.dumps(json.loads(output)))
    else:
        print("Files available for extraction: ",total)
    
    return total

total = file_exists()

# COMMAND ----------

# DBTITLE 1,Get Filename
#Need to differentiate betwen FTPS and ADLS

def get_dir_content(parent_path): 
    for dir_path in dbutils.fs.ls(parent_path):
        print(dir_path)
        if dir_path.isFile():
            mod_time = dir_path.modificationTime/1000
            #print(datetime.fromtimestamp(mod_time))
            yield [dir_path.path, datetime.fromtimestamp(mod_time)]

def get_oldest_modified_file_from_directory(parent_directory):
    directory_contents_list = list(get_dir_content(parent_directory))
    print(directory_contents_list)
    df = spark.createDataFrame(directory_contents_list,['full_file_path', 'last_modified_datetime'])
    min_latest_modified_datetime = df.agg({"last_modified_datetime": "min"}).collect()[0][0]
    print(min_latest_modified_datetime)
    df_filtered = df.filter(df.last_modified_datetime == min_latest_modified_datetime)
    display(df_filtered)
    
    # df_new = df.subtract(df_filtered)
    # for row in df_new.collect():
    #     print('Full Path: ',row.full_file_path)
    #     dbutils.fs.rm(row.full_file_path)
    return df_filtered.first()['full_file_path']

def get_min_source_path(path):
    get_oldest_modified_file_from_directory(path)
    file_name = dbutils.fs.ls(path)[0][1]
    path=path+file_name
    print(path)

    return path

#total = file_exists()
#source_path = get_min_source_path(source_path)

# COMMAND ----------

# DBTITLE 1,Read File Data
def read_file_data(file_format, delimiter, file_header, path):
    print(path)
    if file_format == 'xlsx':
        df1 = pd.read_excel(path, engine= 'openpyxl',sheet_name="Report Data")
        df = spark.createDataFrame(df1)
    else:
        df=spark.read.format(file_format).option("delimiter", delimiter).option("header", file_header).load(path)
    df = df.select([col(column).alias(column.strip()) for column in df.columns])
    display(df)
    return df

# COMMAND ----------

# DBTITLE 1,Rename column
def rename_column(column_list, fix_col, file_nm):
    import datetime
    ins_gmts=datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    if header == 'False':
        import ast
        dict = ast.literal_eval(dic)
        col_rename = []
        for i,j in enumerate(column_list):
            col_rename.append(dict[j])

        print("col_rename: ", *col_rename)

        df_parse=df.filter(lit(col(filter_column)).isNotNull())\
            .filter(lit(col(filter_column)) != " -   ")\
            .withColumn("FILENAME", lit(file_nm))\
            .withColumn("INS_GMTS", lit(ins_gmts))\
            .select("FILENAME",
                    "INS_GMTS",
                    *column_list
                        )
        new_col_rename = fix_col + col_rename
        print("new_col_rename", *new_col_rename)
        df_parse = df_parse.toDF(*new_col_rename)
        
        
    else:
        df_parse=df.filter(lit(col(filter_column)).isNotNull())\
            .filter(lit(col(filter_column)) != " -   ")\
            .withColumn("FILENAME", lit(file_nm))\
            .withColumn("INS_GMTS", lit(ins_gmts))

    display(df_parse)
    return df_parse

# COMMAND ----------

# Custom check to handle null parent/manager_ID for Employees table-keep the existing parent/manager if source parent is null
def employee_null_parent(df):
    if(source_table_name == 'employees'):
        print(target_path)
        if(DeltaTable.isDeltaTable(spark,target_path)):
            target_table_emp = DeltaTable.forPath(spark,target_path)
            df_target = target_table_emp.toDF()
            display(df_target)

            df_parse = df.alias("source").join(df_target.alias("target"),on="employeeNumber",how="left").select("source.*",coalesce("source.parent","target.parent").alias("new_parent"))
            df_parse = df_parse.drop("parent")
            df_parse = df_parse.withColumnRenamed("new_parent","parent")
            display(df_parse)

            print("Before Removing Duplicates:",df_parse.count())
            df_parse = df_parse.dropDuplicates()
            print("After Removing Duplicates:",df_parse.count())
            return df_parse

# COMMAND ----------

# DBTITLE 1,Prepare Delete query
def prep_delete_query(path):
    file_name=path.rsplit('/', 1)[1]
    
    if data_action == 'Truncate':
        query = f"Delete from {target_table}"
    else:
        query = f"Delete from {target_table} where FILENAME='{file_name}'"
    
    print(query)
    return query

# COMMAND ----------

# DBTITLE 1,Write Data
def write_data(df, query):
    if data_action == 'Truncate':
        #spark.sql(query)
        print(df.count())
        df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(target_table,path=target_path)
    elif data_action == 'Append':
        df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(target_table,path=target_path)
    elif data_action == 'Replace':
        if DeltaTable.isDeltaTable(spark, target_path):
                spark.sql(query)
        df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(target_table,path=target_path)
    else:
        print("Wrong Data Action, please specify")
    print(data_action)

# COMMAND ----------

# DBTITLE 1,Archive Data File
def archive_file(file_dir, name):
    import datetime
    
    date=datetime.datetime.now().strftime('%Y%m%d%H')
    archive_path=f"{file_dir}archive/{date}/{name}"
    source_file_dir = f"{file_dir}{name}"
    print(archive_path)
    print(source_file_dir)
    dbutils.fs.mv(source_file_dir,archive_path)

# COMMAND ----------

# DBTITLE 1,Exit Notebook
def exit(File_name):
    process_param = 'Status:{} File Data Loaded Successfully'.format(File_name)
    process_param = process_param.replace(":",'":"')
    output = '{{"{}"}}'.format(process_param)
    dbutils.notebook.exit(json.dumps(json.loads(output)))

# COMMAND ----------

def remove_spaces(df_parse):
    existing_columns = df_parse.columns
    new_columns = [col.replace(" ", "_") for col in existing_columns]
    new_columns = [col.replace("(", "_") for col in new_columns]
    new_columns = [col.replace(")", "_") for col in new_columns]
    df_parse = df_parse.toDF(*new_columns)
    return df_parse


# COMMAND ----------

#call notebooks for each file in location
print("check if files exist in path: ")
source_path = source_path_set_up()
source_file_dir = source_path
target_path = target_path_set_up()
target_table = target_table_set_up()
total = file_exists()
for i in range(total):
    source_path = source_path_set_up()
    source_file_dir = source_path
    target_path = target_path_set_up()
    target_table = target_table_set_up()
    print("Getting min source path:")
    source_path = get_min_source_path(source_path)
    columns = list(col_list.split(","))
    fix_cols = list(fix_column.split(","))
    file_name = source_path.rsplit('/', 1)[1]
    print(file_name)
    print("Reading file data:")
    df = read_file_data(source_fileformat, file_delimiter, header, source_path)
    #Skip to next iteration if the file is empty and delete the file from the source
    if df.isEmpty():
        source_file_dir_delete = f"{source_file_dir}{file_name}"
        dbutils.fs.rm(source_file_dir_delete, False)
        print("The file is empty.")
        continue
    print("Renaming columns:")
    df_parse = rename_column(columns, fix_cols, file_name)
    df_parse = remove_spaces(df_parse)
    if(source_table_name == 'employees'):
        print("Checking for employees with null managers:")
        df_parse = employee_null_parent(df_parse)
    print("Creating the delete query:")
    query = prep_delete_query(source_path)
    print("Writing data to destination:")
    display(df_parse)
    write_data(df_parse, query)
    print("Archiving file:")
    archive_file(source_file_dir, file_name)
    process_param = 'Status:{} File Data Loaded Successfully'.format(file_name)
    print(process_param)

# COMMAND ----------

print("Exiting notebook")
exit(file_name)
