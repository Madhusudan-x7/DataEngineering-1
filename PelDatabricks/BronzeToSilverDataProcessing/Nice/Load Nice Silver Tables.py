# Databricks notebook source
# MAGIC %md # Genric Silver Layer Notebook

# COMMAND ----------

# MAGIC %md ## Importing Libraries

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
import os
import json
from delta.tables import DeltaTable
from ast import literal_eval
import os.path
from os import path

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

# MAGIC %md ##Defining Widgets

# COMMAND ----------

dbutils.widgets.text("UnityCatalogName", "")
dbutils.widgets.text("StorageAccountName", "")
dbutils.widgets.text("ZOrderColumnList", "")
dbutils.widgets.text("SourceBronzePath", "")
dbutils.widgets.text("SourceServiceName", "")
dbutils.widgets.text("SourceEntityName", "")
dbutils.widgets.text("CurrentDate", "")
dbutils.widgets.text("DateSet", "")

# COMMAND ----------

# MAGIC %md ##Loading Widgets

# COMMAND ----------

# DBTITLE 1,setting Input variable values
storage_account_name = dbutils.widgets.get("StorageAccountName")
unity_catlog_name = dbutils.widgets.get("UnityCatalogName")
uc_schema = "pel_nice"
abfss_path = f"abfss://master@{storage_account_name}.dfs.core.windows.net/"
zorder_column_list_value = dbutils.widgets.get("ZOrderColumnList")

#Medallion Changes
# Here source_landing_zone_path represents the landing path.
if "_bronze" in unity_catlog_name:
    source_landing_zone_path = abfss_path + "landing_zone/nice/" + dbutils.widgets.get("SourceBronzePath")
else:
    source_landing_zone_path = abfss_path + "bronze/nice/" + dbutils.widgets.get("SourceBronzePath")

source_entity_name = dbutils.widgets.get("SourceEntityName")
source_service_name = dbutils.widgets.get("SourceServiceName")
date_set = dbutils.widgets.get("DateSet")
current_date = dbutils.widgets.get("CurrentDate")
param_date_set = literal_eval(date_set)
failed_dates = []

# COMMAND ----------

# DBTITLE 1,Setting up silver table name and silver table path
## Medallion changes
create_schema_sql = f"Create schema IF NOT EXISTS {unity_catlog_name}.{uc_schema}"
spark.sql(create_schema_sql)

if "_bronze" in unity_catlog_name:
    bronze_delta_table_path = abfss_path + "bronze/" + source_service_name+'/'+source_entity_name+'/'
    bronze_table_name = unity_catlog_name + "." + uc_schema + "." + source_entity_name
else:
    print('before Medallion changes..')
    bronze_delta_table_path = abfss_path + "silver/" + source_service_name+'/'+source_entity_name+'/'
    bronze_table_name = unity_catlog_name + "." + uc_schema + "." + source_entity_name

# COMMAND ----------

print('bronze_delta_table_path:',bronze_delta_table_path,'\n\nbronze_table_name:',bronze_table_name,'\n\nsource_landing_zone_path:',source_landing_zone_path,'\n\nparam_date_set:',param_date_set)

# COMMAND ----------

if source_entity_name in ("agents", "disposition_skills"):
    print(' Loading for agents or disposition_skills')
    source_files = [r[0] for r in dbutils.fs.ls(source_landing_zone_path)]
    updates_df = spark.read.format("parquet").load(source_files)
    spark.sql("CREATE TABLE IF NOT EXISTS " + bronze_table_name + ' using DELTA LOCATION "' + bronze_delta_table_path +'" ')
    if len(DeltaTable.forPath(spark, bronze_delta_table_path).toDF().columns)>0:
        print(f'regular merge with {len(DeltaTable.forPath(spark, bronze_delta_table_path).toDF().columns)} cols')
        agent_delta = DeltaTable.forPath(spark, bronze_delta_table_path)                  
        (
            agent_delta.alias("t")
            .merge(
                updates_df.withColumn(
                    "silverModifiedDate", to_timestamp(lit(current_date))
                ).alias("s"),
                f"t.{zorder_column_list_value} = s.{zorder_column_list_value}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        spark.sql(
            "OPTIMIZE "
            + bronze_table_name
            + " ZORDER BY ("
            + zorder_column_list_value
            + ")"
        )
    else:
        print('fresh merge')
        updates_df.withColumn(
                    "silverModifiedDate", to_timestamp(lit(current_date))
                ).write.format('delta').mode(
                    "append"
                ).option(
                    "overwriteSchema", "true"
                ).save(
                    bronze_delta_table_path
                )
        agent_delta = DeltaTable.forPath(spark, bronze_delta_table_path)
        agent_delta.delete()
        (
            agent_delta.alias("t")
            .merge(
                updates_df.withColumn(
                    "silverModifiedDate", to_timestamp(lit(current_date))
                ).alias("s"),
                f"t.{zorder_column_list_value} = s.{zorder_column_list_value}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        spark.sql(f"OPTIMIZE {bronze_table_name} ZORDER BY ({zorder_column_list_value}")
        
else:
    print('loading silver Tables')
    for date_set in param_date_set:
        try:
            print('Running for date',date_set["paramValue1"])
            date_set["paramValue1"] = date_set["paramValue1"].replace("Z", "")
            date_set["paramValue2"] = date_set["paramValue2"].replace("Z", "")
            updates_df = spark.read.format("parquet").load(source_landing_zone_path + date_set["paramValue1"])

            updates_col_df = updates_df.withColumn("apiStartDate", to_timestamp(lit(date_set["paramValue1"]))) \
                                       .withColumn("apiEndDate", to_timestamp(lit(date_set["paramValue2"]))) \
                                       .withColumn("silverModifiedDate", to_timestamp(lit(current_date)))

            table_schema = updates_col_df.schema
            if DeltaTable.isDeltaTable(spark, f"{bronze_delta_table_path}"):
                print('table already present')
            else:
                spark.createDataFrame([], table_schema).write.partitionBy("apiStartDate").format("delta").mode("overwrite").option("overwriteSchema", "True") \
                                                       .option("path", f"{bronze_delta_table_path}").saveAsTable(f"{bronze_table_name}")

            spark.sql(
                "DELETE FROM "                                                   
                + bronze_table_name
                + " WHERE apiStartDate = '"
                + date_set["paramValue1"]
                + "' AND apiEndDate = '"
                + date_set["paramValue2"]
                + "'"
                )
            updates_col_df.selectExpr(["*"]).write.partitionBy("apiStartDate").format("delta").mode("append").option("overwriteSchema", "true").save(bronze_delta_table_path)

            spark.sql(
                "OPTIMIZE "
                + bronze_table_name
                + " ZORDER BY ("
                + zorder_column_list_value
                + ")"
                )
        except:
            failed_dates.append(date_set["paramValue1"])

# COMMAND ----------

if failed_dates:
    print('failed_dates',failed_dates)
    raise Exception("dates not found in targated bronze path")
else:
    dbutils.widgets.remove("ZOrderColumnList")
    dbutils.widgets.remove("SourceBronzePath")
    dbutils.widgets.remove("SourceServiceName")
    dbutils.widgets.remove("SourceEntityName")
    dbutils.widgets.remove("CurrentDate")
    dbutils.widgets.remove("DateSet")
