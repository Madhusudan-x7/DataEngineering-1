# Databricks notebook source
# DBTITLE 1,Import Libraries
from delta.tables import DeltaTable
import datetime
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, ShortType, DecimalType, TimestampType, BooleanType, LongType, DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get Set Parameters
dbutils.widgets.text("SourceTable","")
dbutils.widgets.text("TargetTable","")
dbutils.widgets.text("PrimaryKeys","")

source_table = dbutils.widgets.get("SourceTable")
target_table = dbutils.widgets.get("TargetTable")
primary_key = dbutils.widgets.get("PrimaryKeys")

# COMMAND ----------

# DBTITLE 1,Generic Merge
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

# DBTITLE 1,Table Merge
src_df = spark.read.format('delta').table(source_table).drop("File_Date").distinct()

tgt_df = spark.read.format('delta').table(target_table).drop('_change_oper','_rescued_data')

src_columns = src_df.columns

src_columns = [item.lower() for item in src_columns]
tgt_columns = tgt_df.columns

if src_columns != tgt_columns:
    print("Columns not matching")
    
else:
    print("Columns are matching")
    
    if isinstance(primary_key, str):
        primary_key_list = primary_key.replace(" ", "").split(",")
    else:
        primary_key_list = primary_key
    
    
    #print(primary_key_list)
        
    src_df.createOrReplaceTempView("table")
    
    join_condition = " AND ".join([f"trgt.{cn} == src.{cn}" for cn in primary_key_list])

    update_columns = ", ".join([f"trgt.{cn} = src.{cn} " for cn in src_df.columns]) + " , trgt._change_oper = 'U'"

    insert_columns =  ", ".join([cn for cn in src_df.columns]) + ", _change_oper"

    insert_values =   ", ".join([f"src.{cn} " for cn in src_df.columns]) + ", 'I'"
    
    merge_statement = f"""MERGE INTO {target_table} AS trgt
                                  USING table AS src
                                  ON {join_condition}
                                  WHEN MATCHED THEN UPDATE SET {update_columns}
                                  WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
                            """

    #print(merge_statement)

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "table")
    merge_with_retry(src_df, merge_statement)

# COMMAND ----------

# MAGIC %md # Done
