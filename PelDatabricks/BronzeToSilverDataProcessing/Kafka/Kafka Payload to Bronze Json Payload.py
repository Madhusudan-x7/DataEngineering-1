# Databricks notebook source
# MAGIC %md # Kafka Payload to Bronze Json Payload

# COMMAND ----------

# MAGIC %md # Notebook Set Up

# COMMAND ----------

# MAGIC %md ### Function Imports

# COMMAND ----------

from pyspark.sql.functions import col, lit, from_json, max, explode, split, coalesce, regexp_extract, size
from pyspark.sql.window import Window
import json

# COMMAND ----------

# DBTITLE 1,Set Config
spark.conf.set("spark.databricks.delta.properties.defaults.deletedFileRetentionDuration", "interval 30 days")
spark.conf.set("spark.databricks.delta.properties.defaults.checkpointRetentionDuration", "interval 30 days")
spark.conf.set("spark.databricks.delta.properties.defaults.logRetentionDuration", "interval 30 days")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism *16)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

# MAGIC %md ### Utility Functions

# COMMAND ----------

# MAGIC %run "/Order Transparency/UtilityFunctions/Common Utilities"

# COMMAND ----------

# MAGIC %run "../Kafka/Load Generic Kafka Payload"

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

time_out = dbutils.widgets.get("Timeout")
trigger_mode = dbutils.widgets.get("TriggerMode")
control_sql_user = dbutils.widgets.get("ControlSqlUser")
control_secret_scope = dbutils.widgets.get("ControlSecretScope")
control_secret_key = dbutils.widgets.get("ControlSecretKey")
control_sql_server = dbutils.widgets.get("ControlSqlServer")
control_sql_db = dbutils.widgets.get("ControlSqlDb")
kafka_topic = dbutils.widgets.get("KafkaTopic")

timeout_check(time_out)

# COMMAND ----------

control_table_payload_query = f"""
    SELECT 
         payload_table,
         kafka_topic,
         kafka_server,
         truststore_location,
         kafka_secret_scope,
         kafka_secret_user_key,
         kafka_secret_pwd_key,
         min_partitions,
         partition_multiple,
         schedule_pool,
         landing_database,
         landing_schema,
         trigger_mode,
         job_type,
         REPLACE(SUBSTRING(kafka_topic, CHARINDEX('.', kafka_topic) + 1, LEN(kafka_topic)), '-', '_') as source_topic
    FROM etl.ControlTableKafkaPayload
    WHERE is_active = 'Y'
    """


control_table_payload = query_azure_sql(query = control_table_payload_query,
                                user = control_sql_user,
                                scope = control_secret_scope,
                                password = control_secret_key,
                                server = control_sql_server,
                                database = control_sql_db)

display(control_table_payload)

# COMMAND ----------

for row in control_table_payload.collect():
    kafka_server = row.kafka_server
    truststore_location = row.truststore_location
    kafka_secret_scope = row.kafka_secret_scope
    kafka_secret_user_key = row.kafka_secret_user_key
    kafka_secret_pwd_key = row.kafka_secret_pwd_key
    min_partitions = row.min_partitions
    partition_multiple = row.partition_multiple
    kafka_topic = row.kafka_topic
    landing_table = row.payload_table
    landing_database= row.landing_database
    # partition_key = kafka_topic
    partition_key = "kafka_topic"
    source_topic = row.source_topic
    source_topic = source_topic.replace('work_instruction','work_instructions') if source_topic == 'work_instruction' else source_topic
    
    landing_path = f"/mnt/master/bronze/kafka/{landing_table}/data"
    checkpoint_path = f"/mnt/master/bronze/kafka/{landing_table}/checkpoint/{source_topic}"
    # print(landing_path,checkpoint_path,landing_table,landing_database)
    
    trigger_mode = f"{trigger_mode}" 
    
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {landing_database}")

    
    read_kafka(landing_table = landing_table,
               kafka_topic = kafka_topic,
               kafka_server = kafka_server,
               checkpoint_path = checkpoint_path,
               trigger_mode = trigger_mode,
               truststore_location= truststore_location,
               kafka_secret_scope = kafka_secret_scope,
               kafka_secret_user_key = kafka_secret_user_key,
               kafka_secret_pwd_key = kafka_secret_pwd_key,
               min_partitions = min_partitions,
               partition_multiple = partition_multiple,
               landing_database = landing_database,
               landing_path = landing_path,
               partition_key = partition_key,
               source_topic = source_topic
              )

# COMMAND ----------

# DBTITLE 1,Timeout Running Streams
stop_current_streams(time_out)

# COMMAND ----------

# MAGIC %md # Done
