# Databricks notebook source
from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism * 16)
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %run "./Load Generic Kafka"

# COMMAND ----------

dbutils.widgets.text("JobType","")
dbutils.widgets.text("TriggerName","")
dbutils.widgets.text("ControlSqlUser","")
dbutils.widgets.text("ControlSecretScope","")
dbutils.widgets.text("ControlSecretKey","")
dbutils.widgets.text("ControlSqlServer","")
dbutils.widgets.text("ControlSqlDb","")
dbutils.widgets.text("MinPartitions","")
dbutils.widgets.text("Driver","")

job_type = dbutils.widgets.get("JobType")
trigger_name = dbutils.widgets.get("TriggerName")
control_sql_user = dbutils.widgets.get("ControlSqlUser")
control_secret_scope = dbutils.widgets.get("ControlSecretScope")
control_secret_key = dbutils.widgets.get("ControlSecretKey")
control_sql_server = dbutils.widgets.get("ControlSqlServer")
control_sql_db = dbutils.widgets.get("ControlSqlDb")
min_partitions = dbutils.widgets.get("MinPartitions")
driver = dbutils.widgets.get("Driver")

# COMMAND ----------

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

for row in control_table.collect():
    silver_path = row.silver_folder.strip()
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
    
    
    entity_path = f"{silver_path}/{target_table}/data"    
    target_table = database+"."+target_table
    password = dbutils.secrets.get(scope = control_secret_scope, key = pwd_key)
    url = f"jdbc:sqlserver://{server}:{database_port};databaseName={database_name};"
    
    load_sql_kafka_tables(target_table = target_table, 
                          silver_path = entity_path,
                          table = table,
                          user = user,
                          password = password,
                          url = url,
                          partition_column = partition_column,
                          partition_column_value = partition_column_value,
                          driver = driver
                          )
