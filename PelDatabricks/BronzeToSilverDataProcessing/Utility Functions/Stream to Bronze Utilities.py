# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.streaming import DataStreamWriter
from datetime import datetime

# COMMAND ----------

# MAGIC %md ### Common

# COMMAND ----------

def query_azure_sql(query: str, 
                    user: str = "DataLake", 
                    scope: str = "scp-databricks-usc-dev",
                    password: str = "DLSqlCntrl",
                    server: str = "sqlsvr-datalake-usc-dev",
                    database: str = "sqlsvr-dl-control-usc-dev") -> DataFrame:
    """
    Inputs
        query: A SQL query executed against the Azure SQL server / database detailed in future arguments
        user: user name used to authenticate to Azure SQL server
        password: secret key name for password to authenticate to Azure SQL Server
        server: server name to query
        database: database name to query
    Return
        df: A Spark DataFrame of the query results
    Desc
        Logs into the Azure SQL server / database combination detailed in function inputs
        Executes input query against the SQL server
        Returns the query results
        Requires the Scala (Maven) library com.microsoft.azure:spark-mssql-connector_2.12:1.2.0 on the cluster
    """

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

def trigger_ot_stream(streaming_query: DataStreamWriter, 
                      trigger_mode: str) -> None:
    import re
    assert (trigger_mode == "once") or (bool(re.match("\d+ (?:seconds|minutes|hours)", trigger_mode))), \
        print(f"Trigger mode is {trigger_mode}, should either be 'once' or in format '\d+ seconds/minutes/hours'")
        
    if trigger_mode == "once":
        streaming_query.trigger(once = True).start()
    else:
        streaming_query.trigger(processingTime = trigger_mode).start()

# COMMAND ----------

def merge_with_retry(df: DataFrame, 
                     merge_statement: str, 
                     attempts: int = 3) -> None:
    """
    Inputs:
        df: DataFrame used to retrieve _jdf/Spark Session from
        merge_statement: Spark SQL delta merge statement for upsert
        attempts: total number of times to try before error
    Outputs:
        No return value; executes merge statement
    Desc:
        Based on: https://github.com/delta-io/delta/issues/445
    """
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

def stop_all_streams(timeout: int, 
                     attempts: int = 3):
    from time import time, sleep
    
    start_time = time()
    while (time() - start_time < timeout) & (len(spark.streams.active) > 0):
        sleep(1)

    for sstream in spark.streams.active:
        try:
            sstream.stop()
        except Exception as e:
            if attempts > 0:
                attempts += -1
                stop_all_streams(timeout = 1, attempts = attempts)

# COMMAND ----------

# MAGIC %md ### Bronze

# COMMAND ----------

# DBTITLE 1,Translate DFM to Schema
def translate_dfm_to_schema(dfm: list) -> str:
    """
    Small python function to quickly translate .dfm schema to a string that can be copied and used as the text in a Spark schema
    Copy the columns data from the dfm file inside the brackets and pass as input to this function
    It will print out the schema, copy it into the cdc schema notebook
    """
    from pandas import DataFrame as pDataFrame
    from numpy import select as npSelect

    dfm_df = pDataFrame(dfm)

    dfm_types = dfm_df["type"].unique()

    mapped_types = ["INT2", "INT4", "STRING", "CLOB", 
                    "BYTES", "DATETIME", "DATE", "BOOLEAN",
                    "DATE", "NUMERIC"]

    missing_types = [d_t for d_t in dfm_types if d_t not in mapped_types]

    if len(missing_types) > 0:
        assert False, print(f"Data type(s) {missing_types} appear in the .dfm without a mapping to a SparkType. Please add mapping & re-run.")

    mapping_conditions = [(dfm_df["type"]).isin(["INT4", "INT2"]),
                          (dfm_df["type"]).isin(["STRING", "CLOB", "BYTES"]),
                          (dfm_df["type"]).isin(["DATE"]),
                          (dfm_df["type"]).isin(["DATETIME"]),
                          (dfm_df["type"]).isin(["BOOLEAN"]),
                          (dfm_df["type"]).isin(["NUMERIC"])]
    mapping_values = ["IntegerType()",
                      "StringType()",
                      "DateType()", 
                      "TimestampType()",
                      "BooleanType()",
                      "DecimalType(" + dfm_df["precision"].astype(str) + ", " + dfm_df["scale"].astype(str) + ")"
                      ]

    dfm_df["spark_type"] = npSelect(mapping_conditions, mapping_values, None)
    dfm_df.sort_values(by = "ordinal", inplace = True)

    schema = []
    for index, row in dfm_df.iterrows(): 
        schema_val = f'StructField("{row["name"]}", {row["spark_type"]}, True)'
        schema.append(schema_val)

    print(", \n\t".join(schema))

# COMMAND ----------

# DBTITLE 1,Stream to Silver
def stream_to_bronze(landing_path: str,
                     bronze_path: str,
                     schema_path: str,
                     checkpoint_path: str,
                     primary_key: str or list,
                     partition_key: str or list,
                     target_database: str,
                     target_table: str,
                     trigger_mode: str,
                     schedule_pool: str,
                     df_schema: StructType,
                     qlik_schema: StructType = qlik_schema) -> None:
    """
    """
    from pyspark.sql.functions import col, lit
    from delta.tables import DeltaTable
    import re

    ##### Assertions to check that required fields are populated in the control table, meet the appropriate data types #####
    ##### Check paths #####
    assert landing_path is not None, print("Landing source path is missing. Cannot begin with out source location")
    assert bronze_path is not None, print("Bronze path is missing. Cannot begin without sink location")
    assert schema_path is not None, print("Schema path is missing. Cannot begin without schema location")
    assert checkpoint_path is not None, print("Checkpoint path is missing. Stream will not create without checkpoint location")
    ##### Check keys ######
    if primary_key is not None:
        if isinstance(primary_key, str):
            assert primary_key is not None, print("Primary key (string) is missing. Cannot merge without primary key")
        elif isinstance(primary_key, list):
            assert len(primary_key) > 0, print("Primary key (list) is empty. Cannot merge without primary key")
        else:
            assert False, print(f"Primary key should be either string or list. Current type is {type(primary_key)}")

    assert str(type(partition_key)) in ["<class 'str'>", "<class 'list'>", "<class 'NoneType'>"], \
        print(f"Parition key should be a string, list, or None. Current type is {type(partition_key)}")

    ##### Check database & tables #####
    assert target_table is not None, print("Target table is null. Cannot create stream without target details")
    assert target_database is not None, print("Target database is null. Cannot create stream without target details")

    ##### Check to ensure the target database exists, if not create it #####
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_database}")

    ##### Primary keys generally entered as a comma separated string, but could be a list #####
    ##### Allows for reading primary keys both ways and reduces failure likelihood #####
    if primary_key is not None:
        if isinstance(primary_key, str):
            primary_key_list = primary_key.replace(" ", "").split(",")
        else:
            primary_key_list = primary_key
    else:
        primary_key_list = []

    if partition_key is not None:
        if isinstance(partition_key, str):
            partition_key_list = partition_key.replace(" ", "").split(",")
        else:
            partition_key_list = partition_key
    else:
        partition_key = []

    ##### Rows without primary keys will not be considered #####
    if primary_key is not None:
        pk_not_null = " and ".join([f"{pk} is not null" for pk in primary_key_list])
    else:
        pk_not_null = "True"

    ##### Format whatever schema available as DDL to be used to assist streaming inference #####
    df_ddl = ", ".join([" ".join([cn.jsonValue()["name"], cn.jsonValue()["type"]]) for cn in qlik_schema.fields + df_schema.fields])

    ##### Load batch data #####
    ##### If a Bronze table already exists, there will be no batch data process; it is already done #####
    ##### If the Bronze table does not exists, check if there are any files in the full load directory. If none, skip this process #####
    ##### If the files in the landing directory are .parquets, use the standard parquet loader #####
    ##### If the files in the landing directory are .csvs, use the csv loader with the line read options #####
    ##### If a schema was provided, it will be used to read. If not Spark will infer the data types #####
    ##### Once read, change the column names to lower case, ensure data types are correct, filter missing pks, and write #####
    ##### If there is full load data in a new format, this will throw an error. If it does, add code to read in and elif block #####
    if not(DeltaTable.isDeltaTable(spark, bronze_path)):
        #raise Exception("Table might exists already")
        if len([fl.name for fl in dbutils.fs.ls(landing_path)]) > 0:  
            if len([fl.name for fl in dbutils.fs.ls(landing_path) if fl.name.endswith(".parquet")]) > 0:
                batch_in = spark \
                    .read \
                    .format("parquet") \
                    .load(f"{landing_path}/*.parquet")

            elif len([fl.name for fl in dbutils.fs.ls(landing_path) if fl.name.endswith(".csv")]) > 0:
                batch_csv_reader = spark \
                    .read \
                    .format("csv") \
                    .option("header", "true") \
                    .option("quote", "\"") \
                    .option("escape", "\\") \
                    .option("multiLine", "true")\
                    .option("unescapedQuoteHandling","STOP_AT_CLOSING_QUOTE")

                if len(df_schema.fields) > 0:
                    batch_in = batch_csv_reader \
                        .schema(df_schema) \
                        .load(f"{landing_path}/*.csv")
                else:
                    batch_in= batch_csv_reader \
                        .option("inferSchema", True) \
                        .load(f"{landing_path}/*.csv")
            else:
                assert False, print("Batch load prepared for .csv or .parquet files; current batch data was neither. Please review")

            if len(df_schema.fields) > 0:
                batch_select = [col(cn.name).cast(cn.dataType).alias(cn.name.lower()) for cn in df_schema.fields]
            else:
                batch_select = [col(cn.name).alias(cn.name.lower()) for cn in batch_in.schema.fields]

            batch_in = batch_in \
                .select(batch_select)

            non_key_column = []

            for field in batch_in.schema.fields:
                exist_count = [x.lower() for x in primary_key_list].count(field.name)
                if (exist_count == 0 ):
                    non_key_column.append(field.name)

            
            if primary_key_list is not None:
                batch_select_sorted = [x.lower() for x in primary_key_list] + non_key_column
            else:
                batch_select_sorted = non_key_column

            batch_in \
                .select(batch_select_sorted) \
                .where(pk_not_null) \
                .withColumn("_change_oper", lit("I")) \
                .write \
                .format("delta") \
                .mode("error") \
                .partitionBy(partition_key) \
                .option("path", bronze_path) \
                .saveAsTable(f"{target_database}.{target_table}")
        else:
            print(f"\nNo batch data available for table {target_database}.{target_table}\n")


    ##### Read Streaming Data #####        
    stream_reader = spark \
        .readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.inferColumnTypes", True) \
        .option("cloudFiles.schemaHints", df_ddl) \
        .option("cloudFiles.schemaLocation", schema_path) \
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
        .option("header", True) \
        .option("quote", "\"") \
        .option("escape", "\\") \
        .option("multiLine", True)\
        .option("unescapedQuoteHandling","STOP_AT_CLOSING_QUOTE")
    
    try:
        file_level = len([fle.name for fle in dbutils.fs.ls(landing_path + "__ct/") if fle.name.endswith(".csv")])

        if file_level > 0:
            stream_path = f"{landing_path}__ct/*.csv"
        else:
            stream_path = f"{landing_path}__ct/*/*.csv"
            
        stream_in = stream_reader \
            .load(stream_path) \
            .where("header__change_oper != 'B'") \
            .where(pk_not_null)
    except Exception as e:
        if 'UnknownFieldException' in str(e):
            stream_in = stream_reader \
                .load(stream_path) \
                .where("header__change_oper != 'B'") \
                .where(pk_not_null)
        elif 'FileNotFoundException' in str(e):
            print(f"FileNotFoundException: No change files available for {target_database}.{target_table} failed. Please review")
            print("")
            return None
        elif f"Cannot infer schema when the input path `{landing_path}__ct/*/*.csv` is empty" in str(e):
            print(f"Schema Inference Failure: No change files available for {target_database}.{target_table} failed. Please review")
            print("")
            return None
        else:
            print(f"New exception found for table {target_database}.{target_table}.")
            print(f"Error message: {e}")
            return None

    if primary_key is not None:
        write_generic_bronze = stream_in \
            .select([col(cn).alias(cn.lower()) for cn in stream_in.columns]) \
            .writeStream \
            .format("delta") \
            .outputMode("update") \
            .option("mergeSchema", True) \
            .option("checkpointLocation", checkpoint_path) \
            .foreachBatch(lambda df, batch_id: merge_to_bronze(df, batch_id, 
                                                            primary_key = primary_key, 
                                                            partition_key = partition_key,
                                                            bronze_path = bronze_path, 
                                                            target_database = target_database, 
                                                            target_table = target_table, 
                                                            qlik_schema = qlik_schema, 
                                                            schedule_pool = schedule_pool)) \
            .queryName(f"{target_database}.{target_table}")

        trigger_ot_stream(streaming_query = write_generic_bronze,
                        trigger_mode = trigger_mode)
    else:
        write_generic_bronze = stream_in \
            .select([col(cn).alias(cn.lower()) for cn in stream_in.columns]) \
            .writeStream \
            .format("delta") \
            .outputMode("update") \
            .option("mergeSchema", True) \
            .option("checkpointLocation", checkpoint_path) \
            .foreachBatch(lambda df, batch_id: insert_bronze(df, batch_id,
                                                           partition_key = partition_key,
                                                           bronze_path = bronze_path,
                                                           target_database = target_database, 
                                                           target_table = target_table, 
                                                           qlik_schema = qlik_schema, 
                                                           schedule_pool = schedule_pool)) \
        .queryName(f"{target_database}.{target_table}")

        trigger_ot_stream(streaming_query = write_generic_bronze,
                      trigger_mode = trigger_mode)                    

# COMMAND ----------

# DBTITLE 1,Merge to Bronze
def merge_to_bronze(df: DataFrame, 
                    batch_id: str, 
                    primary_key: list,
                    partition_key: list,
                    bronze_path: str, 
                    target_database: str, 
                    target_table: str,
                    qlik_schema: StructType = qlik_schema, 
                    schedule_pool: str = "low") -> None:
    """
    """
    from pyspark.sql.functions import col, last, max
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable

    if isinstance(primary_key, str):
        primary_key_list = primary_key.replace(" ", "").split(",")
    else:
        primary_key_list = primary_key

    if isinstance(partition_key, str):
        partition_key_list = partition_key.replace(" ", "").split(",")
    else:
        partition_key_list = partition_key

    max_window = Window \
        .partitionBy(primary_key_list)

    df_columns = [cn for cn in df.drop("_rescued_data").columns if cn not in [cn.name for cn in qlik_schema]]

    df_max = df.withColumn("_max_seq", max(col("header__change_seq")).over(max_window)) \
        .where("header__change_seq == _max_seq") \
        .select(df_columns + [col("header__change_oper").alias("_change_oper"), "_rescued_data"])

    non_key_column = []

    for field in df_max.schema.fields:
        exist_count = [x.lower() for x in primary_key_list].count(field.name)
        if (exist_count == 0 ):
            non_key_column.append(field.name)

    if primary_key_list is not None:
        batch_select_sorted = [x.lower() for x in primary_key_list] + non_key_column
    else:
        batch_select_sorted = non_key_column

    df_max = df_max.select(batch_select_sorted)

    if DeltaTable.isDeltaTable(spark, bronze_path):
        df_max.createOrReplaceTempView(f"tmp_{target_database}_{target_table}")

        join_condition = " and ".join([f"trgt.{cn} == src.{cn}" for cn in primary_key_list])

        merge_statement = f"""
            merge into {target_database}.{target_table} as trgt
            using tmp_{target_database}_{target_table} as src
            on {join_condition}
            when matched then update set *
            when not matched then insert *
            """

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", schedule_pool)
        merge_with_retry(df_max, merge_statement)

    else:
        #raise Exception ("Table might exists already")
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", schedule_pool)
        df_max \
             .write \
             .format("delta") \
             .mode("error") \
             .partitionBy(partition_key) \
             .option("mergeSchema", True) \
             .option("path", bronze_path) \
             .saveAsTable(f"{target_database}.{target_table}")

# COMMAND ----------

def insert_bronze(df: DataFrame, 
                    batch_id: str, 
                    partition_key: list,
                    bronze_path: str, 
                    target_database: str, 
                    target_table: str,
                    qlik_schema: StructType = qlik_schema, 
                    schedule_pool: str = "low") -> None:
    from pyspark.sql.functions import col, max
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable
    
    df.persist()

    df_columns = [cn for cn in df.drop("_rescued_data").columns if cn not in [cn.name for cn in qlik_schema]]

    df = df \
        .select(df_columns + [col("header__change_oper").alias("_change_oper"), "_rescued_data"])
    
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", schedule_pool)
    if DeltaTable.isDeltaTable(spark, bronze_path):
        df \
            .write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{target_database}.{target_table}")
    
    else:
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", schedule_pool)
        df \
             .write \
             .format("delta") \
             .mode("error") \
             .partitionBy(partition_key) \
             .option("mergeSchema", True) \
             .option("path", bronze_path) \
             .saveAsTable(f"{target_database}.{target_table}")

    df.unpersist()
