# Databricks notebook source
# MAGIC %md # Kafka Table Validation

# COMMAND ----------

# MAGIC %md ## Notebook Set Up

# COMMAND ----------

# MAGIC %md ### Function Imports

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# MAGIC %md ### Utility Functions

# COMMAND ----------

# DBTITLE 1,Load helper functions
# MAGIC %run "./Load Generic Kafka"

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

# DBTITLE 1,Get/Set Parameters
dbutils.widgets.text("ControlSqlUser","")
dbutils.widgets.text("ControlSecretScope","")
dbutils.widgets.text("ControlSecretKey","")
dbutils.widgets.text("ControlSqlServer","")
dbutils.widgets.text("ControlSqlDb","")
dbutils.widgets.text("Driver","")

control_sql_user = dbutils.widgets.get("ControlSqlUser")
control_secret_scope = dbutils.widgets.get("ControlSecretScope")
control_secret_key = dbutils.widgets.get("ControlSecretKey")
control_sql_server = dbutils.widgets.get("ControlSqlServer")
control_sql_db = dbutils.widgets.get("ControlSqlDb")
driver = dbutils.widgets.get("Driver")

# COMMAND ----------

# MAGIC %md ### Dataframe Schema

# COMMAND ----------

# DBTITLE 1,Define Validation dataframe schema
schema = StructType([
  StructField('SourceTableName', StringType(), True),
  StructField('TargetTableName', StringType(), True),
  StructField('SourceBranchNumber', IntegerType(), True),
  StructField('TargetBranchNumber', StringType(), True),
  StructField('SourceCount', IntegerType(), True),
  StructField('TargetCount', LongType(), True)
  ])

validation_df = spark.createDataFrame([], schema)

# COMMAND ----------

# DBTITLE 1,Query Control Data
control_table_query = f"""
    SELECT 
        source_table,
        source_server,
        source_database_port,
        source_database_name,
        source_database_user,
        source_pwd_key,
        source_primary_key,
        silver_database,
        silver_folder,
        target_table,
        trigger_frequency,
        is_active,
        partition_column,
        partition_column_value
    FROM etl.ControlSQLTableKafka
    WHERE is_active = 'Y'
    """

control_table = query_azure_sql(query = control_table_query,
                                user = control_sql_user,
                                scope = control_secret_scope,
                                password = control_secret_key,
                                server = control_sql_server,
                                database = control_sql_db)

display(control_table)

# COMMAND ----------

# DBTITLE 1,Validate Data Count
for row in control_table.collect():

    database = row.silver_database.lower()
    partition_column = row.partition_column.strip()
    pwd_key =  row.source_pwd_key.strip()
    server =  row.source_server.strip()
    database_port =  row.source_database_port
    database_name =  row.source_database_name.strip()
    table =  row.source_table.strip()
    user =  row.source_database_user.strip()
    target_table =  row.target_table.strip()
    partition_column_value = row.partition_column_value.strip()
   
    silver_table = target_table
    
    target_table = database+"."+target_table
    password = dbutils.secrets.get(scope = control_secret_scope, key = pwd_key)
    url = f"jdbc:sqlserver://{server}:{database_port};databaseName={database_name};"
    
    src_cnt_query = f"Select '{silver_table}' as 'SourceTableName', {partition_column_value} as {partition_column}, count(*) as SourceCount from {table}"
    tgt_cnt_query = f"Select '{silver_table}' as TargetTableName, {partition_column}, count(*) as TargetCount from {target_table} where {partition_column}= {partition_column_value} group by {partition_column}"
    
    src_cnt_df = spark.read.format("jdbc")\
                        .option("driver", driver)\
                        .option("url", url)\
                        .option("query", src_cnt_query)\
                        .option("user", user)\
                        .option("password", password)\
                        .load()
    
    tgt_count_df = spark.sql(tgt_cnt_query)
    
    val_df = src_cnt_df.join(tgt_count_df, [src_cnt_df.SourceTableName == tgt_count_df.TargetTableName,src_cnt_df.BranchNumber == tgt_count_df.BranchNumber], "left")\
                              .select("SourceTableName",
                                      "TargetTableName",
                                      src_cnt_df.BranchNumber.alias("SourceBranchNumber"),
                                      tgt_count_df.BranchNumber.alias("TargetBranchNumber"),
                                      "SourceCount",
                                      "TargetCount"
                                     )
    
    validation_df = validation_df.union(val_df)

display(validation_df)
