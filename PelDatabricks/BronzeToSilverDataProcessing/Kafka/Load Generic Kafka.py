# Databricks notebook source
# DBTITLE 1,Import Libraries
#from pyspark.sql.functions import col, lit, substring, substring_index, split, DataFrame, from_json,map_keys, map_values,explode,array,create_map, max, to_utc_timestamp
from delta.tables import DeltaTable
import datetime
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, ShortType, DecimalType, TimestampType, BooleanType, LongType, DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %scala
# MAGIC import net.heartsavior.spark.KafkaOffsetCommitterListener
# MAGIC val listener = new KafkaOffsetCommitterListener()
# MAGIC spark.streams.addListener(listener)

# COMMAND ----------

def query_azure_sql(query: str, 
                    user: str, 
                    scope: str,
                    password: str,
                    server: str,
                    database: str) -> DataFrame:

    password = dbutils.secrets.get(scope = scope, key = password)
    url = f"jdbc:sqlserver://{server}.database.windows.net;databaseName={database};"
    
    df = spark \
        .read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("query", query) \
        .load()
    return df

# COMMAND ----------

def merge_with_retry(df: DataFrame, 
                     merge_statement: str, 
                     attempts: int = 3) -> None:
    
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
    from time import sleep
    import re
    
    try:
        df._jdf.sparkSession().sql(merge_statement)
    except Exception as e:        
        if (attempts > 1) and ("concurrent update" in str(e)) and ("Conflicting commit" in str(e)):
            sleep(15)
            # attempts += -1
            merge_with_retry(df, merge_statement, attempts)
        elif attempts > 1:
            sleep(15)
            attempts += -1
            merge_with_retry(df, merge_statement, attempts)
        else:
            raise

# COMMAND ----------

def trigger_stream(streaming_query: DataStreamWriter, 
                      trigger_mode: str) -> None:
    import re
    assert (trigger_mode == "once") or (bool(re.match("\d+ (?:seconds|minutes|hours)", trigger_mode))), \
        print(f"Trigger mode is {trigger_mode}, should either be 'once' or in format '\d+ seconds/minutes/hours'")
        
    if trigger_mode == "once":
        streaming_query.trigger(once = True).start()
    else:
        streaming_query.trigger(processingTime = trigger_mode).start()


# COMMAND ----------

def timeout_check(timeout: str) -> None:
  
  assert (timeout.rfind(':') != -1) and (timeout.rfind(':') != 0), \
      print(f"Timeout format is {timeout}, should either be in 'H:MM' or 'HH:MM' format ")

  assert (int(timeout[:timeout.rfind(':')])<24) and (int(timeout[timeout.rfind(':')+1:])<60), \
      print(f"Timeout format is {timeout}, hour should be less than 24 and minutes should less than 60")

# COMMAND ----------

def stop_current_streams(timeout: str, 
                     attempts: int = 3) ->str:
    from time import time, sleep
    import datetime
    from pyspark.sql.functions import col, max, unix_timestamp, to_timestamp
    
    hour= ((int(timeout[:timeout.rfind(':')])))
    minutes=(int(timeout[timeout.rfind(':')+1:]))
    curhour= spark.sql(f"""(select int(hour(from_utc_timestamp(current_timestamp,"America/Chicago"))))""").collect()[0][0]
    curmin=  spark.sql(f"""select int(Minute(from_utc_timestamp(current_timestamp,"America/Chicago")))""").collect()[0][0]
    
    if hour<curhour and hour!=0:
        timeout=24 - curhour + hour
        
    elif hour==0:
        timeout=24 - curhour
        
    elif hour>curhour:
        timeout=hour - curhour
        
    elif hour==curhour and minutes>curmin:
        timeout=hour - curhour
        
    elif hour==curhour and minutes<=curmin:
        timeout=24 - curhour + hour
    hour = timeout*60

    
    minutes = minutes-curmin
        
    
    timeout = (hour+(minutes))*60
    start_time = time()
    
    while (time() - start_time < timeout) & (len(spark.streams.active) > 0):
        sleep(10)
    
    
    for sstream in spark.streams.active:
        try:
            sstream.stop()
        except Exception as e:
            if attempts > 0:
                attempts += -1
                stop_all_streams(timeout = 1, attempts = attempts)

# COMMAND ----------

def schema_diff(target_table: str,df1: DataFrame, df2: DataFrame):
    
    s1 = spark.createDataFrame(df1.dtypes, ["d1_name", "d1_type"])
    s2 = spark.createDataFrame(df2.dtypes, ["d2_name", "d2_type"])
    difference = s1.join(s2, s1.d1_name == s2.d2_name, "left")\
                    .where(s2.d2_type.isNull())\
                    .select(s1.d1_name, s1.d1_type)
    
    if difference.count() > 1:
        alter = ""
        for row in difference.collect():
            if alter == "":
                alter = row.d1_name + " " + row.d1_type
            else:
                alter = alter + " , " + row.d1_name + " " + row.d1_type
    
        alter_script = f"Alter table {target_table} add columns ( {alter})"
        spark.sql(alter_script)
    else:
        print("No schema change")

# COMMAND ----------

class CustomError(Exception):
     pass

# COMMAND ----------

def stream_kafka_load(df: DataFrame, 
                             batch_id: str,
                             target_table: str, 
                             table: str,
                             bronze_path: str, 
                             primary_key: str,
                             metadata_topic: str,
                             allow_deletes: str,
                             database: str,
                             partition_multiple: int,
                             partition_column: str,
                             change_feed: str,
                             landing_database: str,
                             landing_path: str) -> None:  
    
    database=database
    table=table
    
    landing_table = f"{landing_database}.{table}"
    landing_table = landing_table.replace("-", "")
    landing_path = landing_path
    
    df = df.cache()
    """df = df.repartition(sc.defaultParallelism * partition_multiple) \
        .cache()"""
    df.count()
    
    try:
        if df.count() != 0:
    
            if isinstance(primary_key, str):
                primary_key_list = primary_key.replace(" ", "").split(",")
            else:
                primary_key_list = primary_key

            max_window = Window \
                .partitionBy(primary_key_list)
         
            ######################################### Read topic to get columns ##############################################
            json_schema = spark.read.json(df.rdd.map(lambda row: row.value)).schema

            src_flatten = df.withColumn('json', from_json(col('value'), schema=json_schema))

            src_data = src_flatten.select("offset","timestamp","json.message.data.*","json.message.headers.operation")
            src_parse = src_data.withColumn("ChangeOperation", col("operation")).withColumn("SparkCreationDateTime", F.current_timestamp()).drop("operation","offset","timestamp")
            json_columns = src_parse.columns

            ######################################### Read table schema to get columns ##############################################

            df_table=spark.read.table(target_table).drop("ChangeOperation","SparkCreationDateTime")
            table_schema=df_table.schema

            dynamic_schema = f"StructType([StructField('headers', StringType(), True),StructField('magic', StringType(), True), StructField('message',StructType([StructField('beforeData', {table_schema} , True), StructField('data',   {table_schema}   , True),  StructField('headers', StructType([StructField('changeMask', StringType(), True), StructField('changeSequence', StringType(), True), StructField('columnMask', StringType(), True), StructField('operation', StringType(), True), StructField('streamPosition', StringType(), True), StructField('timestamp', StringType(), True), StructField('transactionEventCounter', LongType(), True), StructField('transactionId', StringType(), True), StructField('transactionLastEvent', BooleanType(), True)]), True)]), True),  StructField('messageSchema', StringType(), True), StructField('messageSchemaId', StringType(), True), StructField('type', StringType(), True)])"

            df_flatten=df.withColumn('json', from_json(col('value'), schema=eval(dynamic_schema)))
            df_data=df_flatten.select("offset","timestamp","json.message.data.*","json.message.headers.operation")
            df_data_cast=df_data.withColumn("offset", col("offset").cast("Integer"))
            df_max = df_data_cast.withColumn("max_commit", max(col("offset")).over(max_window)) \
                .where("offset == max_commit") \
                .drop(*["timestamp","offset", "max_commit"])

            df_parse=df_max.withColumn("ChangeOperation", col("operation")).withColumn("SparkCreationDateTime", F.current_timestamp()).drop("operation")

            #display(df_parse)

            table_columns = df_parse.columns

            ######################################### Compare columns ##############################################
            if(set(table_columns).issuperset(set(json_columns))):
                table_columns
                #print("Both the schema is matching, loading data to the table")
            else:
                #print("Both the schema is not matching, preparing alter script")
                src_schemaid = src_flatten.select("json.messageSchemaId").distinct()
                schemaId = ""

                ################## Changes-- Remove for loop logic #######################

                y = str(src_schemaid.select('messageSchemaId').rdd.flatMap(lambda x: x).collect())
                schemaId = y.replace('[','(').replace(']',')')

                #display(src_schemaid)

                table_changed = read_schema(target_table,bronze_path,table,kafka_server,metadata_topic,schemaId,truststore_location,kafka_secret_scope,kafka_secret_user_key,kafka_secret_pwd_key,partition_column,change_feed) 
                if table_changed == 1 :
                    table_changed
                    print("Table change detected, rereading the topic to get the change column details")


                #########################################################################

                df_table=spark.read.table(target_table).drop("ChangeOperation","SparkCreationDateTime") 
                table_schema=df_table.schema
                dynamic_schema = f"StructType([StructField('headers', StringType(), True),StructField('magic', StringType(), True), StructField('message',StructType([StructField('beforeData', {table_schema} , True), StructField('data',   {table_schema}   , True),  StructField('headers', StructType([StructField('changeMask', StringType(), True), StructField('changeSequence', StringType(), True), StructField('columnMask', StringType(), True), StructField('operation', StringType(), True), StructField('streamPosition', StringType(), True), StructField('timestamp', StringType(), True), StructField('transactionEventCounter', LongType(), True), StructField('transactionId', StringType(), True), StructField('transactionLastEvent', BooleanType(), True)]), True)]), True),  StructField('messageSchema', StringType(), True), StructField('messageSchemaId', StringType(), True), StructField('type', StringType(), True)])"

                df_flatten=df.withColumn('json', from_json(col('value'), schema=eval(dynamic_schema)))
                df_data=df_flatten.select("offset","timestamp","json.message.data.*","json.message.headers.operation")
                df_data_cast=df_data.withColumn("offset", col("offset").cast("Integer"))
                df_max = df_data_cast.withColumn("max_commit", max(col("offset")).over(max_window)) \
                .where("offset == max_commit") \
                .drop(*["timestamp","offset", "max_commit"])

                df_parse=df_max.withColumn("ChangeOperation", col("operation")).withColumn("SparkCreationDateTime", F.current_timestamp()).drop("operation")

            table = table.replace("-", "")

            df_parse.createOrReplaceTempView(table)

            join_condition = " AND ".join([f"trgt.{cn} == src.{cn}" for cn in primary_key_list])

            update_columns = ", ".join([f"trgt.{cn} = src.{cn} " for cn in df_parse.columns])

            insert_columns =  ", ".join([cn for cn in df_parse.columns])

            insert_values =   ", ".join([f"src.{cn} " for cn in df_parse.columns])

            delete_condition = "(src.ChangeOperation == 'DELETE') "
            
            ###################                Load landing Layer                ###################
            df_data_cast.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(landing_table,path=landing_path)
            
            
            ###################                Load Bronze Layer                ###################
            if (allow_deletes == 'N'):
                merge_statement = f"""MERGE INTO {target_table} AS trgt
                                      USING {table} AS src
                                      ON {join_condition} 
                                      WHEN MATCHED THEN UPDATE SET {update_columns}
                                      WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
                                   """
            else:
                merge_statement = f"""MERGE INTO {target_table} AS trgt
                                      USING {table} AS src
                                      ON {join_condition} 
                                      WHEN MATCHED AND {delete_condition} THEN DELETE
                                      WHEN MATCHED THEN UPDATE SET {update_columns}
                                      WHEN NOT MATCHED AND (src.ChangeOperation != 'DELETE') THEN INSERT ({insert_columns}) VALUES ({insert_values})
                                   """                          

            spark.sparkContext.setLocalProperty("spark.scheduler.pool", table)
            merge_with_retry(df_parse, merge_statement)
    
        else:
            raise CustomError('No Data')
    except CustomError as e: 
        print ('Catched CustomError :{}'.format(e))    

# COMMAND ----------

def read_kafka(target_table: str, 
               table: str,
               bronze_path: str, 
               kafka_topic: str,
               kafka_server: str,
               metadata_topic: str,
               primary_key: str,
               checkpoint_path: str,
               trigger_mode: str,
               allow_deletes: str,
               truststore_location: str,
               kafka_secret_scope: str,
               kafka_secret_user_key: str,
               kafka_secret_pwd_key: str,
               database: str,
               min_partitions: str,
               partition_multiple: int,
               partition_column: str,
               change_feed: str,
               landing_database: str,
               landing_path: str) -> None:
        
    table = table
    groupid = "kf-databricks-"+table
    queryName = "kafka_"+table
    target_table = target_table
    bronze_path = bronze_path
    primary_key = primary_key
    checkpoint_path = checkpoint_path
    metadata_topic = metadata_topic
    allow_deletes = allow_deletes
    min_partitions = min_partitions
    landing_database = landing_database
    landing_path = landing_path
    
    
    if 'dev' in kafka_server:
        read_kafka = spark\
                  .readStream\
                  .format("kafka")\
                  .option("kafka.bootstrap.servers", kafka_server)   \
                  .option("subscribe", kafka_topic)       \
                  .option("startingOffsets", "earliest")\
                  .option("kafka.consumer.commit.groupid", groupid)\
                  .load()
    else:  
        username = dbutils.secrets.get(scope=kafka_secret_scope,key=kafka_secret_user_key)
        password = dbutils.secrets.get(scope=kafka_secret_scope,key=kafka_secret_pwd_key)
        
        EH_SASL = 'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="' + username +'"' +' password="' + password + '";'
        
        read_kafka = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", kafka_server)   \
            .option("kafka.sasl.jaas.config", EH_SASL) \
            .option("kafka.ssl.truststore.location",truststore_location) \
            .option("subscribe", kafka_topic)       \
            .option("startingOffsets", "earliest")\
            .option("kafka.sasl.mechanism", "SCRAM-SHA-256")\
            .option("kafka.security.protocol", "SASL_SSL")\
            .option("kafka.consumer.commit.groupid", groupid)\
            .option("failOnDataLoss", False)\
            .option("minPartitions", min_partitions)\
            .load()

    write_kafka= read_kafka.selectExpr("CAST(Key AS STRING)","CAST(value AS STRING)","CAST(offset AS STRING)","CAST(timestamp AS STRING)")\
                  .writeStream \
                  .format("delta") \
                  .outputMode("append") \
                  .option("checkpointLocation", checkpoint_path)\
                  .option ("failOnDataLoss", "false")\
                  .foreachBatch(lambda df, batch_id: stream_kafka_load(df, batch_id,
                                                                       target_table,
                                                                       table,
                                                                       bronze_path,
                                                                       primary_key,
                                                                       metadata_topic,
                                                                       allow_deletes,
                                                                       database,
                                                                       partition_multiple,
                                                                       partition_column,
                                                                       change_feed,
                                                                       landing_database,
                                                                       landing_path)) \
                   .queryName(queryName)\
                   
    
    trigger_stream(streaming_query = write_kafka,
                  trigger_mode = trigger_mode)

# COMMAND ----------

def read_schema(target_table: str, 
                bronze_path: str, 
                table: str,
                kafka_server: str,
                metadata_topic: str,
                schemaId: str,
                truststore_location: str,
                kafka_secret_scope: str,
                kafka_secret_user_key: str,
                kafka_secret_pwd_key: str,
                partition_column: str,
                change_feed: str) -> None:
    
    if 'dev' in kafka_server:
        kafka = spark.read \
                      .format("kafka") \
                      .option("kafka.bootstrap.servers", kafka_server)   \
                      .option("subscribe", metadata_topic)    \
                      .option("startingOffsets", "earliest") \
                      .load()
    else:
        username = dbutils.secrets.get(scope=kafka_secret_scope,key=kafka_secret_user_key)
        password = dbutils.secrets.get(scope=kafka_secret_scope,key=kafka_secret_pwd_key)
        
        EH_SASL = 'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="' + username +'"' +' password="' + password + '";'
        
        kafka = spark.read.format("kafka")\
            .option("kafka.bootstrap.servers", kafka_server)   \
            .option("kafka.sasl.jaas.config", EH_SASL) \
            .option("kafka.ssl.truststore.location",truststore_location) \
            .option("subscribe", metadata_topic)       \
            .option("startingOffsets", "earliest")\
            .option("kafka.sasl.mechanism", "SCRAM-SHA-256")\
            .option("kafka.security.protocol", "SASL_SSL")\
            .load()
 
    df=kafka.selectExpr("CAST(Key AS STRING)","CAST(value AS STRING)")
    df.persist()
    json_schema = spark.read.json(df.rdd.map(lambda row: row.value)).schema
    df_schema=df.withColumn('json', from_json(col('value'), json_schema))
    #display(df_schema)
    df_schema.persist()
    
    if (schemaId == "null") :
        filter_condition = f"table = '{table}'"
    else:
        filter_condition = f"schemaId in {schemaId}"
    
    df_filter=df_schema.select("json.message.schemaId","json.message.lineage.table").filter(filter_condition)
    df_final=df.join(df_filter, df.Key == df_filter.schemaId, "inner").select("value").distinct()

    json_schema = spark.read.json(df_final.rdd.map(lambda row: row.value)).schema
    df_value=df_final.withColumn('json', from_json(col('value'), json_schema))
    
    df_flatten=df_value.select("json.message.schemaId","json.message.lineage.table","json.message.tableStructure.tableColumns","json.message.lineage.tableVersion").orderBy(col("tableVersion").desc()).limit(1)
    
    df_data=df_flatten.select(col("tableColumns.*"))
    
    
    x=df_data.columns
    schema = ""
    alter_schema = ""
    for row in x:
                data_type = f"{row}.type"
                data_nullable = f"{row}.nullable"
                nullable = df_data.select(data_nullable).rdd.map(lambda x : x[0]).collect()
                type=str(df_data.select(data_type).rdd.map(lambda x : x[0]).collect())
                alter_type=str(df_data.select(data_type).rdd.map(lambda x : x[0]).collect())
                if str(type) == "['NUMERIC']":
                    data_scale = f"{row}.scale"
                    data_precision = f"{row}.precision"
                    scale = str(df_data.select(data_scale).rdd.map(lambda x : x[0]).collect())
                    precision = str(df_data.select(data_precision).rdd.map(lambda x : x[0]).collect())
                    type = "Decimal("+precision+","+scale+")"
                    alter_type = "DecimalType("+precision+","+scale+")"
                schema_val = f"{row} {type}"
                alter_schema_val = f'StructField("{row}", {alter_type} , {nullable})'
                if schema == "":
                    schema = schema + schema_val
                    alter_schema = alter_schema + alter_schema_val
                else:
                    schema = schema + " , " + schema_val
                    alter_schema = alter_schema + " , " + alter_schema_val
    
    table_schema = schema.replace("[", "")\
                        .replace("]", "").replace("'", "")\
                        .replace("INT8", "INT")\
                        .replace("INT4", "INT")\
                        .replace("INT2", "INT") \
                        .replace("BOOLEAN", "Boolean")\
                        .replace("UINT1", "Integer")\
                        .replace("REAL4", "Double")\
                        .replace("REAL8", "Double")\
                        .replace("CLOB", "String")\
                        .replace("BLOB", "String")\
                        .replace("DATETIME", "Timestamp")
    
    alter_table_schema = alter_schema.replace("[", "")\
                        .replace("]", "").replace("'", "")\
                        .replace("STRING", "StringType()")\
                        .replace("INT8", "IntegerType()")\
                        .replace("INT4", "IntegerType()")\
                        .replace("INT2", "IntegerType()") \
                        .replace("BOOLEAN", "BooleanType()")\
                        .replace("UINT1", "IntegerType()")\
                        .replace("REAL4", "DoubleType()")\
                        .replace("REAL8", "DoubleType()")\
                        .replace("CLOB", "StringType()")\
                        .replace("BLOB", "StringType()")\
                        .replace("DATETIME", "TimestampType()")
    
    final_schema = " StructType([ " +  alter_table_schema + ", StructField(\"ChangeOperation\", StringType(), True), StructField(\"SparkCreationDateTime\", TimestampType(), True)]) "
    
    if (partition_column == "null") :
        Create_query = f"Create Table " +target_table + " (" +  table_schema + ", ChangeOperation String, SparkCreationDateTime Timestamp ) LOCATION '"+bronze_path+"' TBLPROPERTIES('delta.deletedFileRetentionDuration' = 'interval "+change_feed+" days', 'delta.enableChangeDataFeed' = 'true')"
    else:
        Create_query = f"Create Table " +target_table + " (" +  table_schema + ", ChangeOperation String, SparkCreationDateTime Timestamp ) PARTITIONED BY ( "+ partition_column+") LOCATION '"+bronze_path+"' TBLPROPERTIES('delta.deletedFileRetentionDuration' = 'interval "+change_feed+" days', 'delta.enableChangeDataFeed' = 'true')"
    
    if DeltaTable.isDeltaTable(spark, bronze_path):
        print("Entity table exists, comparing both Schema")
        old_df=spark.read.table(target_table)
        new_df=spark.createDataFrame([], schema=eval(final_schema))
        diff_schema=schema_diff(target_table,new_df,old_df)
    else:
        print("Entity table does not exist, creating empty structure to merge into")
        spark.sql(Create_query)
        
    df.unpersist()
    df_schema.unpersist()
    return 1

# COMMAND ----------

def load_sql_kafka_tables(target_table: str, 
                          bronze_path: str, 
                          table: str, 
                          user: str, 
                          password: str, 
                          url: str, 
                          partition_column: str,
                          partition_column_value: str,
                          driver: str) -> None:
    
    df = spark.read.format("jdbc")\
                        .option("driver", driver)\
                        .option("url", url)\
                        .option("dbtable", table)\
                        .option("user", user)\
                        .option("password", password)\
                        .load()
    
    write_df = df.withColumn(partition_column, lit(partition_column_value))
    
    write_df.write \
           .format("delta") \
           .mode("append") \
           .partitionBy(partition_column)\
           .saveAsTable(target_table,path=bronze_path)
