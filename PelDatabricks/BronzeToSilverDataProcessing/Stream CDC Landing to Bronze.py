# Databricks notebook source
# MAGIC %md # Stream CDC Bronze to Silver

# COMMAND ----------

# MAGIC %md # Notebook Set Up

# COMMAND ----------

# MAGIC %md ### Spark Configurations & Function Imports

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.deletedFileRetentionDuration", "interval 365 days")
spark.conf.set("spark.databricks.delta.properties.defaults.checkpointRetentionDuration", "interval 365 days")
spark.conf.set("spark.databricks.delta.properties.defaults.logRetentionDuration", "interval 365 days")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
spark.conf.set("spark.sql.streaming.stopTimeout", "300s")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ### Table Schemas

# COMMAND ----------

# MAGIC %run "./Schemas/CDC Silver Schemas"

# COMMAND ----------

# MAGIC %md ### Utility Functions

# COMMAND ----------

# MAGIC %run "./Utility Functions/Stream to Bronze Utilities"

# COMMAND ----------

# MAGIC %run "../Order Transparency/UtilityFunctions/Common Utilities"

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

# timeout = 60 * int(dbutils.widgets.get("TimeoutMinutes"))
timeout = dbutils.widgets.get("Timeout")
timeout_check(timeout)
job_type = dbutils.widgets.get("JobType")
trigger_name = dbutils.widgets.get("TriggerName")
user = dbutils.widgets.get("SqlUser")
scope = dbutils.widgets.get("SecretScope")
password = dbutils.widgets.get("SecretKey")
server = dbutils.widgets.get("SqlServer")
database = dbutils.widgets.get("SqlDb")
source_filter = dbutils.widgets.get("SourceFilter")

if source_filter == "":
    source_filter = 'is not null'

# COMMAND ----------

# MAGIC %md # Stream Processing

# COMMAND ----------

# MAGIC %md ### Control Data

# COMMAND ----------

# DBTITLE 1,Query Control Data
control_table_query = f"""
    SELECT extraction_method
        , source_name
        , source_table
        , source_primary_key
        , landing_container
        , landing_folder
        , landing_file
        , bronze_container
        , bronze_folder
        , bronze_file
        , target_database
        , target_table
        , trigger_frequency
        , trigger_interval
        , schedule_pool
        , partition_column
        , optimize_column
    FROM etl.ControlTableBronze
    WHERE is_active = 'Y'
        AND job_type = '{job_type}'
        AND trigger_name = '{trigger_name}'
        AND source_name {source_filter}
    """

control_table = query_azure_sql(query = control_table_query,
                                user = user,
                                scope = scope,
                                password = password,
                                server = server,
                                database = database)

display(control_table)

# COMMAND ----------

# MAGIC %md ### Stream to Silver

# COMMAND ----------

# DBTITLE 1,Create All Streams
for row in control_table.collect():
    source_system = row.source_name.lower()
    source_table = row.source_table.strip()
    primary_key = row.source_primary_key.strip() if row.source_primary_key is not None else None
    partition_key = row.partition_column.strip() if row.partition_column is not None else None
    
    landing_path = f"/mnt/{row.landing_container}/{row.landing_folder}/{row.landing_file}"
    schema_path = f"/mnt/{row.bronze_container}/{row.bronze_folder}/{row.bronze_file}/schema"
    checkpoint_path = f"/mnt/{row.bronze_container}/{row.bronze_folder}/{row.bronze_file}/checkpoints"
    bronze_path = f"/mnt/{row.bronze_container}/{row.bronze_folder}/{row.bronze_file}/data"
    
    target_database = row.target_database    
    target_table = row.target_table
    
    trigger_mode = f"{row.trigger_frequency} {row.trigger_interval}" if job_type == "stream" else "once"    
    schedule_pool = row.schedule_pool
    
    try:
        exec(f"df_schema = {source_system}_{target_table}_schema")
    except Exception as e:
        if isinstance(e, NameError):
            print(f"No schema available for {source_system}_{target_table}, will infer data types without hint\n")
            df_schema = StructType([])
        else:
            raise e
    
    # print(f"Creating stream for {source_table}, reading from bronze at {bronze_path}")
    # print(f"Schema details saved at {schema_path}")
    # print(f"Stream will be written to ADLS at {silver_path}")
    # print(f"Checkpoints will be written to {checkpoint_path}")
    # print(f"Stream will be written to unmanaged table {target_database}.{target_table} and merged by {primary_key}")
    # print("")
    
    stream_to_bronze(landing_path = landing_path,
                     bronze_path = bronze_path,
                     schema_path = schema_path,
                     checkpoint_path = checkpoint_path,
                     primary_key = primary_key,
                     partition_key = partition_key,
                     target_database = target_database,
                     target_table = target_table,
                     trigger_mode = trigger_mode,
                     schedule_pool = schedule_pool,
                     df_schema = df_schema,
                     qlik_schema = qlik_schema)

# COMMAND ----------

# DBTITLE 1,Timeout Running Streams
if job_type =='stream':
    stop_current_streams(timeout)

# COMMAND ----------

# MAGIC %md # Done
