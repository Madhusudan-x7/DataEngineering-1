# Databricks notebook source
# MAGIC %md # Kafka landing zone to Bronze

# COMMAND ----------

# MAGIC %md # Notebook Set Up

# COMMAND ----------

# MAGIC %md ### Function Imports

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Set Config
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism * 16)
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.streaming.stopTimeout", "300s")

# COMMAND ----------

# MAGIC %scala
# MAGIC import net.heartsavior.spark.KafkaOffsetCommitterListener
# MAGIC val listener = new KafkaOffsetCommitterListener()
# MAGIC spark.streams.addListener(listener)

# COMMAND ----------

# MAGIC %md ### Utility Functions

# COMMAND ----------

# MAGIC %run "./Load Generic Kafka"

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

dbutils.widgets.text("Timeout","")
dbutils.widgets.text("TriggerMode","")
dbutils.widgets.text("JobType","")
dbutils.widgets.text("TriggerName","")
dbutils.widgets.text("ControlSqlUser","")
dbutils.widgets.text("ControlSecretScope","")
dbutils.widgets.text("ControlSecretKey","")
dbutils.widgets.text("ControlSqlServer","")
dbutils.widgets.text("ControlSqlDb","")
dbutils.widgets.text("KafkaServer","")
dbutils.widgets.text("MetadataTopic","")
dbutils.widgets.text("TruststoreLocation","")
dbutils.widgets.text("KafkaSecretScope","")
dbutils.widgets.text("KafkaSecretUserKey","")
dbutils.widgets.text("KafkaSecretPwdKey","")
dbutils.widgets.text("MinPartitions","")

time_out = dbutils.widgets.get("Timeout")
trigger_mode = dbutils.widgets.get("TriggerMode")
job_type = dbutils.widgets.get("JobType")
trigger_name = dbutils.widgets.get("TriggerName")
control_sql_user = dbutils.widgets.get("ControlSqlUser")
control_secret_scope = dbutils.widgets.get("ControlSecretScope")
control_secret_key = dbutils.widgets.get("ControlSecretKey")
control_sql_server = dbutils.widgets.get("ControlSqlServer")
control_sql_db = dbutils.widgets.get("ControlSqlDb")
kafka_server = dbutils.widgets.get("KafkaServer")
metadata_topic = dbutils.widgets.get("MetadataTopic")
truststore_location = dbutils.widgets.get("TruststoreLocation")
kafka_secret_scope = dbutils.widgets.get("KafkaSecretScope")
kafka_secret_user_key = dbutils.widgets.get("KafkaSecretUserKey")
kafka_secret_pwd_key = dbutils.widgets.get("KafkaSecretPwdKey")
min_partitions = dbutils.widgets.get("MinPartitions")

timeout_check(time_out)

if min_partitions =="":
    min_partitions = "4"

# COMMAND ----------

# DBTITLE 1,Query Control Data
control_table_query = f"""
    SELECT 
         source_topic
        , source_primary_key
        , bronze_folder  
        , bronze_database
        , landing_database
        , landing_folder
        , target_table
        , trigger_frequency
        , allow_deletes
        , partition_multiple
        , partition_column
        , change_feed
    FROM etl.ControlTableKafka
    WHERE is_active = 'Y'
        AND job_type = '{job_type}'
        AND trigger_name = '{trigger_name}'
    """

control_table = query_azure_sql(query = control_table_query,
                                user = control_sql_user,
                                scope = control_secret_scope,
                                password = control_secret_key,
                                server = control_sql_server,
                                database = control_sql_db)

display(control_table)

# COMMAND ----------

# DBTITLE 1,Create Delta Tables
for row in control_table.collect():
    bronze_path = row.bronze_folder.strip()
    table = row.target_table.lower()
    database = row.bronze_database.lower()
    partition_column = row.partition_column.strip()
    change_feed = row.change_feed.strip()
    landing_database = row.landing_database.strip()
    
    table_path = table.replace("-", "")
    entity_path = f"{bronze_path}/{table_path}/data"
    checkpoint_path = f"{bronze_path}/{table_path}/checkpoint/"
    
    target_table = database+"."+table_path
    
    if DeltaTable.isDeltaTable(spark, entity_path):
        print("Entity table exists")
        
    else:
        print("Entity table does not exist")
        landing_database_query=f"Create database if not exists {landing_database}"
        spark.sql(landing_database_query)
        bronze_database_query=f"Create database if not exists {database}"
        spark.sql(bronze_database_query)
        
        print(target_table)
        read_schema(target_table = target_table, 
                    bronze_path = entity_path,  
                    table = table,
                    kafka_server = kafka_server,
                    metadata_topic = metadata_topic,
                    schemaId = "null" ,
                    truststore_location= truststore_location,
                    kafka_secret_scope = kafka_secret_scope,
                    kafka_secret_user_key = kafka_secret_user_key,
                    kafka_secret_pwd_key = kafka_secret_pwd_key,
                    partition_column = partition_column,
                    change_feed = change_feed
                   )

# COMMAND ----------

# DBTITLE 1,Load Kafka Tables
for row in control_table.collect():
    landing_path = row.landing_folder.strip()
    kafka_topic = row.source_topic.strip()
    primary_key = row.source_primary_key.strip()
    allow_deletes = row.allow_deletes.strip()
    table = row.target_table.lower()
    database = row.bronze_database.lower()
    partition_multiple = row.partition_multiple
    partition_column = row.partition_column.strip()
    change_feed = row.change_feed.strip()
    landing_database = row.landing_database.strip()
    landing_folder = row.landing_folder.strip()
    
    
    
    table_path = table.replace("-", "")
    entity_path = f"{bronze_path}/{table_path}/data"
    landing_path = f"{landing_folder}/{table_path}/data"
    checkpoint_path = f"{bronze_path}/{table_path}/checkpoint/"
    target_table = database+"."+table_path
    min_partitions = min_partitions
    
    read_kafka(target_table = target_table, 
               table = table,
               bronze_path = entity_path, 
               kafka_topic = kafka_topic,
               kafka_server = kafka_server,
               metadata_topic = metadata_topic,
               primary_key = primary_key,
               checkpoint_path = checkpoint_path,
               trigger_mode = trigger_mode,
               allow_deletes = allow_deletes,
               truststore_location= truststore_location,
               kafka_secret_scope = kafka_secret_scope,
               kafka_secret_user_key = kafka_secret_user_key,
               kafka_secret_pwd_key = kafka_secret_pwd_key,
               database = database,
               min_partitions = min_partitions,
               partition_multiple = partition_multiple,
               partition_column = partition_column,
               change_feed = change_feed,
               landing_database = landing_database,
               landing_path = landing_path
              )

# COMMAND ----------

# DBTITLE 1,Timeout Running Streams
stop_current_streams(time_out)


# COMMAND ----------

# MAGIC %md # Done
