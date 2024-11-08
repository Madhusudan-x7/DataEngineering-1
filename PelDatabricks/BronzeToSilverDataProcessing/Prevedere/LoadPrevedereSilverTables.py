# Databricks notebook source
# MAGIC %md #Genric Silver Layer Notebook

# COMMAND ----------

# MAGIC %md ##Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import lit, col, cast
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType, TimestampType,DateType
import requests
import json

# COMMAND ----------

# MAGIC %md ##Schemas

# COMMAND ----------

series_provider_schema = StructType([
    StructField("Value", FloatType(), nullable=True),
    StructField("Date", TimestampType(), nullable=True),
    StructField("Annotation", StringType(), nullable=True),
    StructField("ManuallyAdjusted", StringType(), nullable=True),
    StructField("IsForecasted", StringType(), nullable=True),
    StructField("ProviderId", StringType(), nullable=True),
    StructField("RunTime", TimestampType(), nullable=True)
])

# COMMAND ----------

provider_metadata_schema = StructType([
    StructField("Id", StringType(), nullable=False),
    StructField("Name", StringType(), nullable=True),
    StructField("Description", StringType(), nullable=True),
    StructField("Aggregate", StringType(), nullable=True),
    StructField("Calculation", StringType(), nullable=True),
    StructField("Color", StringType(), nullable=True),
    StructField("DisplayInformation", StringType(), nullable=True),
    StructField("Count", StringType(), nullable=True),
    StructField("Created", StringType(), nullable=True),
    StructField("StartTime", StringType(), nullable=True),
    StructField("EndTime", StringType(), nullable=True),
    StructField("TransformedStartTime", StringType(), nullable=True),
    StructField("TransformedEndTime", StringType(), nullable=True),
    StructField("Frequency", StringType(), nullable=True),
    StructField("Deprecated", StringType(), nullable=True),
    StructField("DeprecationReason", StringType(), nullable=True),
    StructField("LastModified", StringType(), nullable=True),
    StructField("Provider.Name", StringType(), nullable=True),
    StructField("Provider.Id", StringType(), nullable=True),
    StructField("Provider.Description", StringType(), nullable=True),
    StructField("Provider.Source", StringType(), nullable=True),
    StructField("ProviderId", StringType(), nullable=True),
    StructField("Seasonality", StringType(), nullable=True),
    StructField("Source", StringType(), nullable=True),
    StructField("Classification", StringType(), nullable=True),
    StructField("Tags", StringType(), nullable=True),
    StructField("Type", StringType(), nullable=True),
    StructField("Units", StringType(), nullable=True),
    StructField("Accessibility", StringType(), nullable=True),
    StructField("PeriodAlignment", StringType(), nullable=True),
    StructField("Path", StringType(), nullable=True),
    StructField("Notes", StringType(), nullable=True),
    StructField("Citation", StringType(), nullable=True),
    StructField("DataSources.Name", StringType(), nullable=True),
    StructField("DataSources.Url", StringType(), nullable=True),
    StructField("DataSources.DataSourceType", StringType(), nullable=True),
    StructField("IndustryCodes", StringType(), nullable=True),
    StructField("HasZeroVariance", StringType(), nullable=True),
    StructField("Interval", StringType(), nullable=True)
])


# COMMAND ----------

# MAGIC %md ##Utility_Functions

# COMMAND ----------

def get_prev(provider, providerid):
    ep = f'https://api.prevedere.com/indicator/{provider}/{providerid}?ApiKey={pkey}' 
    headers = {
        'Content-Type': 'application/json',
    }
    res = requests.get(ep, headers=headers)
    res2 = json.loads(res.content.decode('utf-8'))
    if isinstance(res2,dict):
        mylist = [res2]
        return (mylist)
    else:
        return res2

# COMMAND ----------

def extract_nested_values(d):
    new_dict = {}
 
    list(map(lambda item: new_dict.update({f"{item[0]}.{k}": v if v else None for k, v in item[1].items()}) if isinstance(item[1], dict) else
             (new_dict.update({f"{item[0]}.{l}": m if m else None for l, m in item[1][0].items()}) if isinstance(item[1], list) and item[1] and isinstance(item[1][0], dict) else
              (new_dict.update({item[0]: ', '.join(map(str, item[1]))}) if isinstance(item[1], list) else new_dict.update({item[0]: item[1]}))
              ), d.items()))
 
    return [new_dict]

# COMMAND ----------

# def extract_nested_values(d):
#     new_dict = {}
    
#     list(map(lambda item: new_dict.update({f"{item[0]}.{k}": v if v else None for k, v in item[1].items()}) if isinstance(item[1], dict) else 
#     (new_dict.update({item[0]: ', '.join(map(str, item[1]))}) if isinstance(item[1], list) else new_dict.update({item[0]: item[1]})
#     ), d.items()))
    
#     return [new_dict]

# COMMAND ----------

# MAGIC %md ##Constants

# COMMAND ----------

ProviderId = dbutils.widgets.get("ProviderId")
RunTime = dbutils.widgets.get("RunTime")
LandingZonePath = dbutils.widgets.get("LandingZonePath")

StorageAccountName = dbutils.widgets.get("StorageAccountName")
CatalogName = dbutils.widgets.get("CatalogName")
Scope = dbutils.widgets.get("Scope")
bronze_folder_name = dbutils.widgets.get("BronzeFolderName")

Key = dbutils.widgets.get("Key")
Provider = dbutils.widgets.get("Provider")

abfss_path = f"abfss://master@{StorageAccountName}.dfs.core.windows.net/"
source_landing_zone_path  = abfss_path + f"{LandingZonePath}/*" 
schema_name = 'prevedere'
source_entity_name = f'series_{ProviderId}'
bronze_delta_table_path = abfss_path + f"{bronze_folder_name}/prevedere/series_{ProviderId}/"
bronze_table_name_schema = CatalogName + "." + schema_name + "." + source_entity_name

#Metadata Constants
source_entity_name_metadata = 'indicator_metadata'
bronze_metadata_path = abfss_path + f"{bronze_folder_name}/prevedere/{source_entity_name_metadata}/"
metadata_table = CatalogName + "." + schema_name + "." + source_entity_name_metadata
provider = Provider
pkey = dbutils.secrets.get(scope = Scope ,key=Key)

# COMMAND ----------

series_df = spark.read.parquet(source_landing_zone_path)
series_df = series_df.withColumn("ProviderId", lit(ProviderId)) \
        .withColumn("RunTime", lit(RunTime))

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, bronze_delta_table_path):
    print('Table Already Exists appending the data ')
else:
    # Create an empty DataFrame with the specified schema
    empty_df = spark.createDataFrame([], series_provider_schema)

    # Write the DataFrame as a Delta table
    empty_df.write.format("delta") \
               .mode("overwrite") \
               .option("overwriteSchema", "true") \
               .save(bronze_delta_table_path)

    # Register the Delta table
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {bronze_table_name_schema} USING delta LOCATION '{bronze_delta_table_path}'""")

# Appending data to the Delta table after changing schema
batch_select = [col(cn.name).cast(cn.dataType) for cn in series_provider_schema]
series_df = series_df.select(batch_select)

series_df.select(batch_select).write.format("delta") \
                         .mode("append") \
                         .insertInto(f"{CatalogName}.{schema_name}.{source_entity_name}")

# COMMAND ----------

# MAGIC %md ##Indicator Metadata

# COMMAND ----------

if DeltaTable.isDeltaTable(spark,bronze_metadata_path ):
    df = spark.read.table(metadata_table)
    resultdf = df.filter(col("ProviderId") == ProviderId)
    if resultdf.count() > 0:
        print('provider id already exists in the table')
    else:
        results= get_prev(provider, ProviderId )
        result = list(map(lambda x: extract_nested_values(x)[0], results))
        #print(result)
        df = spark.createDataFrame(result, schema=provider_metadata_schema)
        display(df)
        df.write.format("delta") \
                         .mode("append") \
                         .option("overwriteSchema", "true") \
                         .insertInto(f"{CatalogName}.{schema_name}.{source_entity_name_metadata}")
else:
    #Create an empty DataFrame with the specified schema
    empty_meta_df = spark.createDataFrame([], provider_metadata_schema)

    # Write the DataFrame as a Delta table
    empty_meta_df.write.format("delta") \
               .mode("overwrite") \
               .option("overwriteSchema", "true") \
               .save(bronze_metadata_path)


    # Register the Delta table
    spark.sql("CREATE TABLE IF NOT EXISTS  " + metadata_table + ' using DELTA LOCATION "' + bronze_metadata_path +'"')
    results= get_prev(provider, ProviderId )
    result = list(map(lambda x: extract_nested_values(x)[0], results))
    df = spark.createDataFrame(result, schema=provider_metadata_schema)
    df.write.format("delta") \
        .mode("append") \
        .insertInto(f"{CatalogName}.{schema_name}.{source_entity_name_metadata}")

# COMMAND ----------


