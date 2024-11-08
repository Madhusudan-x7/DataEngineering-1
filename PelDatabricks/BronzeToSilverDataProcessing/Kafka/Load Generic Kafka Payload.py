# Databricks notebook source
# DBTITLE 1,Import Libraries
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, ShortType, DecimalType, TimestampType, BooleanType, LongType, DoubleType
from pyspark.sql.functions import col, lit, from_json, max, explode, split, regexp_replace, json_tuple, expr, concat, coalesce, array,current_timestamp,map_from_arrays,arrays_zip
import json
import datetime
import re
import time

# COMMAND ----------

class CustomError(Exception):
     pass

# COMMAND ----------

def read_kafka(landing_table: str,
               kafka_topic: str,
               kafka_server: str,
               checkpoint_path: str,
               trigger_mode: str,
               truststore_location: str,
               kafka_secret_scope: str,
               kafka_secret_user_key: str,
               kafka_secret_pwd_key: str,
               min_partitions: str,
               partition_multiple: int,
               landing_database: str,
               landing_path: str,
               partition_key: str,
               source_topic :str) -> None:
        

    groupid = "kf-databricks-"+landing_table
    queryName = "kafka_"+source_topic
    checkpoint_path = checkpoint_path
    min_partitions = min_partitions
    landing_database = landing_database
    landing_path = landing_path
    landing_table = landing_table
    
    
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
        # st = {"tracking.work-instruction":{"0":4067980}}
        # json.dumps(st)
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
            .option("includeHeaders", "true")\
            .load()

    kafka_data = read_kafka.withColumn("kafka_topic",lit(kafka_topic)).selectExpr("kafka_topic","CAST(Key AS STRING)","CAST(value AS STRING)","CAST(offset AS STRING)","CAST(timestamp AS STRING)","CAST(headers as STRING)","CAST(partition AS STRING)")

    kafka_data = kafka_data.withColumn("application_name", regexp_extract(col("headers"), r"applicationName,\s*([^},]+)", 1)) \
            .withColumn("transaction_type", regexp_extract(col("headers"), r"transactionType,\s*([^}]+)", 1))
    
    kafka_data = kafka_data.select("kafka_topic","Key","value","offset","partition","headers","application_name","transaction_type","timestamp")
    
    write_kafka= kafka_data \
                  .writeStream \
                  .format("delta") \
                  .outputMode("append") \
                  .option("checkpointLocation", checkpoint_path)\
                  .option ("failOnDataLoss", "false")\
                  .foreachBatch(lambda df, batch_id: stream_kafka_load(df, batch_id,
                                                                       landing_table,
                                                                       landing_database,
                                                                       landing_path,
                                                                       partition_key)) \
                   .queryName(queryName)\
                   
    trigger_ot_stream(streaming_query = write_kafka,
                  trigger_mode = trigger_mode)

# COMMAND ----------

def stream_kafka_load(df: DataFrame, 
                             batch_id: str,
                             landing_table: str,
                             landing_database: str,
                             landing_path: str,
                             partition_key: str) -> None:  
    

    
    landing_table = f"{landing_database}.{landing_table}"
    landing_path = landing_path

    df = df.repartition(sc.defaultParallelism * 8) \
        .cache()
    df.count()
    
    try:
        if df.count() != 0:

            df_final = df.withColumn("SparkCreationDateTime", current_timestamp())
            
            df_final.write.mode("append").format("delta").option("mergeSchema", "true").partitionBy(partition_key).saveAsTable(landing_table,path=landing_path)
            
    
        else:
            raise CustomError('No Data')
    except CustomError as e: 
        print ('Catched CustomError :{}'.format(e))    

# COMMAND ----------

def convertSnakeCase(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

# COMMAND ----------

@udf
def splitdata (parsed_data :str,tables:str):
    jsondata = json.loads(parsed_data)
    return json.dumps(jsondata.get(tables))

# COMMAND ----------

def get_schema(df,col):
    schema = spark.read.json(df.rdd.map(lambda x: x[col])).schema
    if schema != StructType([]):
        sch='array<struct<'
        for s in schema.names:
            sch+=f'{s}: string,'
        sch=sch[:-1]
        sch+='>>'
    else:
        sch = ''
    return sch

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType

def generate_schema_old(schema):
    if schema != StructType([]):
        schema_str = ""
        for field in schema.fields:
            if isinstance(field.dataType, ArrayType):
                inner_struct = generate_schema(StructType(field.dataType.elementType.fields))
                schema_str += f"{field.name}: array<struct<{inner_struct}>>, "
            elif isinstance(field.dataType, StructType):
                inner_struct = generate_schema(field.dataType)
                schema_str += f"{field.name}: struct<{inner_struct}>, "
            else:
                schema_str += f"{field.name}: {field.dataType.typeName()}, "
    else:
        schema_str = ''
    return schema_str.rstrip(", ")


# COMMAND ----------

def get_type_name(data_type):
    """
    Get the type name for the given data type.
    
    Args:
    - data_type: DataType object
    
    Returns:
    - str: String representation of the data type
    """
    if isinstance(data_type, StringType):
        return "string"
    elif isinstance(data_type, BooleanType):
        return "boolean"
    elif isinstance(data_type, LongType):
        return "long"
    else:
        return data_type.typeName()

def generate_schema(schema):
    """
    Generate schema recursively based on the given schema structure.
    
    Args:
    - schema: StructType object representing the schema structure
    
    Returns:
    - str: String representation of the dynamically generated schema
    """
    schema_str = ""
    for field in schema.fields:
        if isinstance(field.dataType, ArrayType):
            if isinstance(field.dataType.elementType, StringType):
                schema_str += f"{field.name}: array<string>, "
            else:
                inner_struct = generate_schema(StructType(field.dataType.elementType.fields))
                schema_str += f"{field.name}: array<struct<{inner_struct}>>, "
        elif isinstance(field.dataType, StructType):
            inner_struct = generate_schema(field.dataType)
            schema_str += f"{field.name}: struct<{inner_struct}>, "
        else:
            schema_str += f"{field.name}: {get_type_name(field.dataType)}, "

    return schema_str.rstrip(", ")

# COMMAND ----------

def drop_duplicate_columns(df):
    # Create a new DataFrame with renamed duplicate columns
    renamed_columns = []
    seen_columns = set()
    for column in df.columns:
        lowercase_column = column.lower()
        if lowercase_column in seen_columns:
            renamed_columns.append(column + "_duplicate")
        else:
            seen_columns.add(lowercase_column)
            renamed_columns.append(column)
    new_df = df.toDF(*renamed_columns)
    
    # Drop the duplicate columns
    columns_to_drop = [col + "_duplicate" for col in seen_columns]
    new_df = new_df.drop(*columns_to_drop)
    
    return new_df

# COMMAND ----------

def recursive_explode_final(df):
    
    array_cols = [c for c, t in df.dtypes if t.startswith("array")]
    
    while len(array_cols)>0:
        for cn in array_cols:
            df = df.select(df["*"],explode(df[cn])).select(df['*'],'col.*').drop(cn)
        array_cols = [c for c, t in df.dtypes if t.startswith("array")]
    return df


# COMMAND ----------

@udf
def json_parsing(json_string: str):
    table_value= {}
    dataRecord_dict={}
    new_data = []
    jsondata = json.loads(json_string)
    fields=jsondata['dataRecord'].keys()  #table name
    
    for tbl in fields:
        if isinstance(jsondata['dataRecord'][tbl],list) or isinstance(jsondata['dataRecord'][tbl],dict):
            if isinstance(jsondata['dataRecord'][tbl],dict):
                data = [jsondata['dataRecord'][tbl]]

            else:
                data = jsondata['dataRecord'][tbl]
            if data:

                if tbl not in table_value:
                    table_value[tbl] = data if data else []
                else:
                    table_value[tbl] = (table_value[tbl] if table_value[tbl] else [])+ (data if data else [])
        else:
            dataRecord_dict[tbl] = jsondata['dataRecord'][tbl]

    new_data += [dataRecord_dict]
    new_data = list({tuple(sorted(d.items())): d for d in new_data}.values())
    if new_data:
        tbl='dataRecord'
        if tbl not in table_value:
            table_value[tbl] = new_data if new_data else []
        else:
            table_value[tbl] = (table_value[tbl] if table_value[tbl] else [])+ (new_data if new_data else [])
    
    return(json.dumps(table_value))

# COMMAND ----------

def load_kafka_bronze(landing_table: str,
                      landing_database: str,
                      transaction_type: str,
                      schedule_pool: str,
                      source_topic: str) -> None:
    
    bronze_folder = f"/mnt/master/bronze/kafka/{source_topic}"
    trans_type = transaction_type.replace('-','_').replace(',','_').replace(' ','')
    bronze_checkpoint_path = f"{bronze_folder}/checkpoint/{trans_type}"

    
    if isinstance(transaction_type, str):
        transaction_type_list = transaction_type.split(",")
    else:
        transaction_type_list = transaction_type
    # print(transaction_type_list)
    stream_payload = spark \
            .readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp","2024-07-31")\
            .table(f"{landing_database}.{landing_table}") \
            .where((col("transaction_type")).isin(transaction_type_list)) \
            .select(col("Key").cast("string"),
                    col("value").cast("string"),
                    col("offset").cast("string"),
                    col("timestamp").cast("string")) 
    
            
    write_stream_payload = stream_payload \
            .writeStream \
            .format("delta") \
            .outputMode("update") \
            .option("mergeSchema", True) \
            .option("checkpointLocation", bronze_checkpoint_path) \
            .foreachBatch(lambda df, batch_id: insert_bronze(df, batch_id, schedule_pool = schedule_pool, landing_table=landing_table, transaction_type = transaction_type)) \
    .queryName(f"write_bronze_kafka_json_payload_{source_topic}_{trans_type}")
    
    trigger_ot_stream(streaming_query = write_stream_payload,
                    trigger_mode = trigger_mode)   
    
    
                 

# COMMAND ----------

def insert_bronze(df: DataFrame, 
                             batch_id: str,
                             schedule_pool: str,
                             landing_table: str,
                             transaction_type:str) -> None:  
    
    from pyspark.sql.functions import col, last, max
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable
    
    
    df = df \
        .cache()
    df.count()

    js_parsed_data  = df.withColumn("parsed_data",json_parsing(col("value")))

    js_parsed_data = js_parsed_data.persist()
    js_parsed_data.count()
    
    if js_parsed_data.count()>0:
        for row in control_table.where(f"payload_table='{landing_table}'").where(f"transaction_type='{transaction_type}'").collect():
            table_value= {}
            dataRecord_dict={}
            new_data = []
            start_time = time.time()
            source_primary_key = row.source_primary_key.lower()
            primary_key = row.source_primary_key.strip() if row.source_primary_key is not None else None
            partition_key = row.partition_key.strip() if row.partition_key is not None else None
            bronze_folder = row.bronze_folder
            bronze_path = f"{row.bronze_folder}/{row.target_table}/data"
            source_key_explode = row.source_key_explode
            
            landing_database = row.landing_database
            landing_table = row.payload_table
            target_database = row.bronze_database  
            source_key_pair = row.source_key_pair
            nested_key_pair = row.nested_key_pair  
            # key_pair = row.nested_key_pair if row.nested_key_pair is not None else row.source_key_pair
            key_pair = row.source_key_pair
            target_table = row.target_table
            # print( target_database,target_table,key_pair)
            partition_multiple = 1
            is_merge = row.is_merge
            key_value = js_parsed_data.select("key").rdd.map(lambda x: x[0].split(':')[0]).first()
            key_value_d = key_value + '_d'
            nested_pair_primary_key = row.nested_pair_primary_key.strip() if row.nested_pair_primary_key is not None else None
            if isinstance(nested_pair_primary_key, str):
                nest_key_value_list = nested_pair_primary_key.replace(" ", "").split(",")
            else:
                nest_key_value_list = nested_pair_primary_key
            
            tblname = convertSnakeCase(row.source_key_pair) if row.nested_key_pair is None else convertSnakeCase(row.nested_key_pair)+'_'+ convertSnakeCase(row.source_key_pair)
            # print(tblname,source_key_pair,convertSnakeCase(row.source_key_pair))
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_database}")
            dataframes = {}
            js_parsed_data = js_parsed_data.withColumn(key_pair,splitdata(col("parsed_data"),lit(key_pair))).withColumn(key_value_d,split(js_parsed_data['key'], ':').getItem(1).cast("string"))
            df_tbl = ""
            df_tbl = js_parsed_data.select("offset",key_value_d,key_pair).where(key_pair + """ not in ('[{}]','null') """)
            df_tbl = df_tbl.cache()
            df_tbl.count()
            # display(df_tbl)
            extract_schema = ""
            extract_schema = spark.read.json(df_tbl.rdd.map(lambda row: row[key_pair])).schema
            schema = ""
            schema = f"array<struct<{generate_schema(extract_schema)}>>"
            # print(target_table,schema)
            
            if source_key_explode == 'Y' and not(nest_key_value_list):
                # print(1)
                res = df_tbl.withColumn("value_parsed", expr(f"from_json({key_pair}, '{schema}')")).drop(key_pair)
                df_explode = recursive_explode_final(res)
            
            elif schema and (source_key_explode == 'N' or nest_key_value_list):
                # print(2)
                res = df_tbl.withColumn("value_parsed", expr(f"from_json({key_pair}, '{schema}')"))
                df_explode = res.select('offset',key_value_d,explode(res.value_parsed)).select('offset',key_value_d,'col.*')
            
            else:
                df_explode = spark.createDataFrame([],StructType([StructField(key_value_d, StringType())]))


            if nested_key_pair in df_explode.columns:
                # print('nested')
                df_explode = df_explode.filter(size(col(nested_key_pair)) > 0)
                if df_explode.count()>0:
                    cols_check = df_explode.select(explode(df_explode[nested_key_pair])).select('col.*')
                    not_exist_cols = [col for col in nest_key_value_list if col not in cols_check.columns]
                    not_exist_cols

                    if not_exist_cols:
                        nesteddf2=df_explode.select('offset',key_value_d,nested_key_pair,*not_exist_cols).drop("_corrupt_record")
                    else:
                        nesteddf2=df_explode.select('offset',key_value_d,nested_key_pair).drop("_corrupt_record")


                    if source_key_explode == 'Y':
                        # print("exp")
                        nesteddf2 = recursive_explode_final(nesteddf2)
                    
                    else:
                        # print("not exp")
                        nesteddf2 = df_explode.select('offset',key_value_d,explode(df_explode[nested_key_pair])).select('offset',key_value_d,'col.*').drop("_corrupt_record")
                
                else:
                    nesteddf2 = spark.createDataFrame([],StructType([StructField(key_value_d, StringType())]))
            
            else:
                nesteddf2 = spark.createDataFrame([],StructType([StructField(key_value_d, StringType())]))
            

            if nested_key_pair:
                df_fin = nesteddf2
            elif not(nested_key_pair):
                df_fin = df_explode
            
            df_fin = drop_duplicate_columns(df_fin)
            # display(df_fin)
            if isinstance(primary_key, str):
                primary_key_list = primary_key.replace(" ", "").split(",")
            else:
                primary_key_list = primary_key

            if partition_key is not None:
                if isinstance(partition_key, str):
                    partition_key_list = partition_key.replace(" ", "").split(",")
                else:
                    partition_key_list = partition_key
            else:
                partition_key = []
            
            
            max_window = Window \
                        .partitionBy(primary_key_list)
            

            join_condition = " and ".join([f"trgt.{cn} == src.{cn}" for cn in primary_key_list])

            data_not_null = " or ".join([f"{pk} is not null" for pk in df_fin.drop(*["offset","commit"]).columns if pk in primary_key_list])

            pk_not_null = " and ".join([f"{pk} is not null" for pk in primary_key_list])

            passthorugh_columns = len([col_name for col_name in df_fin.columns if col_name in (key_value_d,key_value)]) > 1
            
            if passthorugh_columns:
                df_fin = df_fin.withColumn(key_value,coalesce(key_value,key_value_d)).drop(key_value_d)
            else:
                df_fin = df_fin.withColumn(key_value,col(key_value_d)).drop(key_value_d)
            
            df_cols = df_fin.toDF(*[col_name.lower() for col_name in df_fin.columns])
            
            if all(cols.lower() in df_cols.columns for cols in primary_key_list):
                
                df_final = df_fin.where(pk_not_null).distinct()
                
                if df_final.count()>0 and is_merge=='Y':

                    df_max = df_final.withColumn("commit",col("offset").cast("int"))\
                            .withColumn("max_commit", max(col("commit")).over(max_window)) \
                            .where("commit == max_commit") \
                            .drop(*["offset", "max_commit","commit","_corrupt_record"])
                    
                    # batch_select = [col(cn).alias(cn[0].capitalize() + cn[1:]) for cn in  df_max.columns]
                    batch_select = [col(cn).cast("string").alias(cn[0].capitalize() + cn[1:]) for cn in  df_max.columns]

                    df_load = df_max.where(pk_not_null).select(batch_select) 


                    df_load.createOrReplaceTempView(f"tmp_{target_database}_{target_table}")
                    
                
                    update_columns = ", ".join([f"trgt.{cn} = src.{cn} " for cn in df_load.drop(*["offset"]).columns])

                    insert_columns =  ", ".join([cn for cn in df_load.drop(*["offset"]).columns])

                    insert_values =   ", ".join([f"src.{cn} " for cn in df_load.drop(*["offset"]).columns])

                    spark.sparkContext.setLocalProperty("spark.scheduler.pool", schedule_pool)
                    if DeltaTable.isDeltaTable(spark, bronze_path):
                        merge_statement = f"""
                            merge into {target_database}.{target_table} as trgt
                            using tmp_{target_database}_{target_table} as src
                            on {join_condition}
                            WHEN MATCHED THEN UPDATE SET *
                            WHEN NOT MATCHED THEN INSERT *
                            """
                        
                        merge_with_retry(df_load, merge_statement)

                    else:           
                        df_load \
                            .write \
                            .format("delta") \
                            .mode("error") \
                            .partitionBy(partition_key) \
                            .option("mergeSchema", True) \
                            .option("path", bronze_path) \
                            .saveAsTable(f"{target_database}.{target_table}")
                        
                elif df_final.count()>0 and is_merge=='N':
                
                    df_max = df_final\
                            .drop(*["offset", "max_commit","commit","_corrupt_record"])

                    # batch_select = [col(cn).alias(cn[0].capitalize() + cn[1:]) for cn in  df_max.columns]
                    batch_select = [col(cn).cast("string").alias(cn[0].capitalize() + cn[1:]) for cn in  df_max.columns]
                    
                    df_load = df_max.where(pk_not_null).select(batch_select) 

                    if DeltaTable.isDeltaTable(spark, bronze_path):
                        df_load \
                            .write \
                            .format("delta") \
                            .mode("append") \
                            .option("mergeSchema", True) \
                            .saveAsTable(f"{target_database}.{target_table}")
                    
                    else:
                        df_load \
                            .write \
                            .format("delta") \
                            .mode("error") \
                            .option("mergeSchema", True) \
                            .partitionBy(partition_key) \
                            .option("path", bronze_path) \
                            .saveAsTable(f"{target_database}.{target_table}")
            df_tbl.unpersist()
            df_explode.unpersist()
            df_fin.unpersist()
        js_parsed_data.unpersist()
        df.unpersist()

# COMMAND ----------

# def insert_bronze(df: DataFrame, 
#                              batch_id: str,
#                              schedule_pool: str,
#                              landing_table: str) -> None:  
    
#     from pyspark.sql.functions import col, last, max
#     from pyspark.sql.window import Window
#     from delta.tables import DeltaTable
    
    
#     df = df \
#         .cache()
#     df.count()

#     js_parsed_data  = df.withColumn("parsed_data",json_parsing(col("value")))

#     js_parsed_data = js_parsed_data.persist()
#     js_parsed_data.count()
    
#     if js_parsed_data.count()>0:
#         for row in control_table.where(f"payload_table='{landing_table}'").collect():
#             start_time = time.time()
#             source_primary_key = row.source_primary_key.lower()
#             primary_key = row.source_primary_key.strip() if row.source_primary_key is not None else None
#             partition_key = row.partition_key.strip() if row.partition_key is not None else None
#             bronze_folder = row.bronze_folder
#             bronze_path = f"{row.bronze_folder}/{row.target_table}/data"
            
#             landing_database = row.landing_database
#             landing_table = row.payload_table
#             target_database = row.bronze_database  
#             source_key_pair = row.source_key_pair
#             nested_key_pair = row.nested_key_pair  
#             key_pair = row.nested_key_pair if row.nested_key_pair is not None else row.source_key_pair
#             target_table = row.target_table
#             # print( target_database,target_table)
#             partition_multiple = 1
#             is_merge = row.is_merge
#             key_value = js_parsed_data.select("key").rdd.map(lambda x: x[0].split(':')[0]).first()
#             key_value_d = key_value + '_d'
#             nested_pair_primary_key = row.nested_pair_primary_key
#             if isinstance(nested_pair_primary_key, str):
#                 nest_key_value_list = nested_pair_primary_key.replace(" ", "").split(",")
#             else:
#                 nest_key_value_list = nested_pair_primary_key
#             tblname = convertSnakeCase(row.source_key_pair) if row.nested_key_pair is None else convertSnakeCase(row.nested_key_pair)+'_'+ convertSnakeCase(row.source_key_pair)
#             # print(tblname,source_key_pair,convertSnakeCase(row.source_key_pair))
#             spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_database}")
#             dataframes = {}
#             js_parsed_data = js_parsed_data.withColumn(key_pair,splitdata(col("parsed_data"),lit(key_pair))).withColumn(key_value_d,split(js_parsed_data['key'], ':').getItem(1).cast("string"))
#             df_tbl = js_parsed_data.select("offset",key_value_d,key_pair).where(key_pair + """ not in ('[{}]','null') """)
#             df_tbl = df_tbl.cache()
#             df_tbl.count()
#             schema=get_schema(df_tbl,key_pair)
#             if schema:
#                 res = df_tbl.withColumn("value_parsed", expr(f"from_json({key_pair}, '{schema}')"))
#                 df_explode = res.select('offset',key_value_d,explode(res.value_parsed)).select('offset',key_value_d,'col.*')
#             else:
#                 df_explode = spark.createDataFrame([],StructType([StructField(key_value_d, StringType())]))
                
#             if source_key_pair in df_explode.columns:
#                 nested_schema=get_schema(df_explode,source_key_pair)
#                 if nested_schema:
#                     nesteddf = df_explode.withColumn("nested_parsed", expr(f"from_json({source_key_pair}, '{nested_schema}')"))
#                     cols_check = nesteddf.select(explode(nesteddf.nested_parsed)).select('col.*')
#                     not_exist_cols = [col for col in nest_key_value_list if col not in cols_check.columns]
#                     if not_exist_cols:
#                         nesteddf2=nesteddf.select('offset',key_value_d,explode(nesteddf.nested_parsed),*not_exist_cols).select('offset',key_value_d,'col.*',*not_exist_cols).drop("_corrupt_record")
#                     else:
#                         nesteddf2=nesteddf.select('offset',key_value_d,explode(nesteddf.nested_parsed)).select('offset',key_value_d,'col.*').drop("_corrupt_record")
#                 else:
#                     nesteddf2 = spark.createDataFrame([],StructType([StructField(key_value_d, StringType())]))
#             else:
#                 nesteddf2 = spark.createDataFrame([],StructType([StructField(key_value_d, StringType())]))

#             if nested_key_pair:
#                 df_fin = nesteddf2
#             elif not(nested_key_pair):
#                 df_fin = df_explode
            
#             # display(df_fin)
#             if isinstance(primary_key, str):
#                 primary_key_list = primary_key.replace(" ", "").split(",")
#             else:
#                 primary_key_list = primary_key

#             if partition_key is not None:
#                 if isinstance(partition_key, str):
#                     partition_key_list = partition_key.replace(" ", "").split(",")
#                 else:
#                     partition_key_list = partition_key
#             else:
#                 partition_key = []
            
            
#             max_window = Window \
#                         .partitionBy(primary_key_list)
            

#             join_condition = " and ".join([f"trgt.{cn} == src.{cn}" for cn in primary_key_list])

#             data_not_null = " or ".join([f"{pk} is not null" for pk in df_fin.drop(*["offset","commit"]).columns if pk in primary_key_list])

#             pk_not_null = " and ".join([f"{pk} is not null" for pk in primary_key_list])
            
#             df_cols = df_fin.toDF(*[col_name.lower() for col_name in df_fin.columns])

#             passthorugh_columns = len([col_name for col_name in df_fin.columns if col_name in (key_value_d,key_value)]) > 1
            
#             if passthorugh_columns:
#                 df_fin = df_fin.withColumn(key_value,coalesce(key_value,key_value_d)).drop(key_value_d)
#             else:
#                 df_fin = df_fin.withColumn(key_value,col(key_value_d)).drop(key_value_d)
            
#             if all(cols.lower() in df_cols.columns for cols in primary_key_list):
                
#                 df_final = df_fin.where(pk_not_null).distinct()
                
#                 if df_final.count()>0 and is_merge=='Y':

#                     df_max = df_final.withColumn("commit",col("offset").cast("int"))\
#                             .withColumn("max_commit", max(col("commit")).over(max_window)) \
#                             .where("commit == max_commit") \
#                             .drop(*["offset", "max_commit","commit","_corrupt_record"])
                    
#                     batch_select = [col(cn).alias(cn[0].capitalize() + cn[1:]) for cn in  df_max.columns]

#                     df_load = df_max.where(pk_not_null).select(batch_select) 


#                     df_load.createOrReplaceTempView(f"tmp_{target_database}_{target_table}")
                    
                
#                     update_columns = ", ".join([f"trgt.{cn} = src.{cn} " for cn in df_load.drop(*["offset"]).columns])

#                     insert_columns =  ", ".join([cn for cn in df_load.drop(*["offset"]).columns])

#                     insert_values =   ", ".join([f"src.{cn} " for cn in df_load.drop(*["offset"]).columns])

#                     spark.sparkContext.setLocalProperty("spark.scheduler.pool", schedule_pool)
#                     if DeltaTable.isDeltaTable(spark, bronze_path):
#                         merge_statement = f"""
#                             merge into {target_database}.{target_table} as trgt
#                             using tmp_{target_database}_{target_table} as src
#                             on {join_condition}
#                             WHEN MATCHED THEN UPDATE SET *
#                             WHEN NOT MATCHED THEN INSERT *
#                             """
                        
#                         merge_with_retry(df_load, merge_statement)

#                     else:           
#                         df_load \
#                             .write \
#                             .format("delta") \
#                             .mode("error") \
#                             .partitionBy(partition_key) \
#                             .option("mergeSchema", True) \
#                             .option("path", bronze_path) \
#                             .saveAsTable(f"{target_database}.{target_table}")
                        
#                 elif df_final.count()>0 and is_merge=='N':
                
#                     df_max = df_final\
#                             .drop(*["offset", "max_commit","commit","_corrupt_record"])

#                     batch_select = [col(cn).alias(cn[0].capitalize() + cn[1:]) for cn in  df_max.columns]
                    
#                     df_load = df_max.where(pk_not_null).select(batch_select) 

#                     if DeltaTable.isDeltaTable(spark, bronze_path):
#                         df_load \
#                             .write \
#                             .format("delta") \
#                             .mode("append") \
#                             .option("mergeSchema", True) \
#                             .saveAsTable(f"{target_database}.{target_table}")
                    
#                     else:
#                         df_load \
#                             .write \
#                             .format("delta") \
#                             .mode("error") \
#                             .option("mergeSchema", True) \
#                             .partitionBy(partition_key) \
#                             .option("path", bronze_path) \
#                             .saveAsTable(f"{target_database}.{target_table}")
#             df_tbl.unpersist()
#             df_explode.unpersist()
#             df_fin.unpersist()
#         js_parsed_data.unpersist()
#         df.unpersist()
