# Databricks notebook source
# MAGIC %md # Notebook Set Up

# COMMAND ----------

# MAGIC %md ### Function Imports

# COMMAND ----------

# DBTITLE 1,Import Libraries
import requests
import os,tarfile
from pyspark.sql import functions as F
import pandas as pd
import re
import json
import time
from IPython.display import clear_output
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

# MAGIC %md ### Constants

# COMMAND ----------

# DBTITLE 1,Get Set Parmeters

dbutils.widgets.text("DataLakeDestinationContainer","")
dbutils.widgets.text("StorageAccountName","")
dbutils.widgets.text("DirectFileSourceExtractionMethod","")
dbutils.widgets.text("StartRange","")
dbutils.widgets.text("Endrange","")
dbutils.widgets.text("TargetFilePath","")
dbutils.widgets.text("CatalogSchema","")
dbutils.widgets.text("AuthTokken","")


catalog_Schema = dbutils.widgets.get("CatalogSchema")
container_name = dbutils.widgets.get("DataLakeDestinationContainer")
storage_account_name = dbutils.widgets.get("StorageAccountName")
direct_file_source_extraction_method = dbutils.widgets.get("DirectFileSourceExtractionMethod")
target_file_path = dbutils.widgets.get("TargetFilePath")
start_range = dbutils.widgets.get("StartRange")
end_range = dbutils.widgets.get("Endrange")
auth_tokken = dbutils.widgets.get("AuthTokken")

abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

if direct_file_source_extraction_method == 'FULL':
      table_name = f"{catalog_Schema}.pel_osc.osc_fattach_file_logs_{start_range}_{end_range}"
      spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ( event_name STRING,  event_id INT,  starttime TIMESTAMP ) USING delta LOCATION '{abfss_path}bronze/OSC/{table_name.replace('prod_bronze.pel_osc.', '')}'")
if direct_file_source_extraction_method == 'DELTA':
      table_name = f"{catalog_Schema}.pel_osc.osc_fattach_file_logs_delta"
      spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ( event_name STRING,  event_id INT,  starttime TIMESTAMP ) USING delta LOCATION '{abfss_path}bronze/OSC/{table_name.replace('prod_bronze.pel_osc.', '')}'")

print( container_name,'\n',storage_account_name,'\n',abfss_path,'\n',target_file_path,'\n',direct_file_source_extraction_method,'\n',table_name)

# COMMAND ----------

# MAGIC %md ### Utility Functions

# COMMAND ----------

def listToString(s):
    return "".join(s)

# This function (extract_files_with_retry) extract files from a given URL with retry logic in case of failures. 
# It handles the extraction of attachments from a given URL and saves them to the specified target directory.

def extract_files_with_retry(url: str, max_retries: int = 2) -> None:
    parent_url = []
    for attempt in range(max_retries + 1):
        try:
            event = re.findall(r'\d+', url)
            event_id = listToString(event)
            event_name = url.split('/')[7]
            
            payload = {}
            headers = {
                'OSvC-CREST-Application-Context': 'Fetch Attachments',
                'Authorization': f'{auth_tokken}'
            }
            response = requests.request("GET", url, headers=headers, data=payload , timeout=1800)

            if response.status_code == 200:
                filename = response.headers['Content-Disposition'].split("filename=")[-1]
                filename = filename.replace("\"", "")

                # Assuming target_file_path is defined
                directory_path = f'{target_file_path}{event_name}/{event_id}'

                file_path = f'{target_file_path}{event_name}/{event_id}/{filename}'
                dbutils.fs.mkdirs(directory_path)

                zippedfile = open(file_path, 'wb')
                zippedfile.write(response.content)
                zippedfile.close()

                tar = tarfile.open(file_path, 'r')
                for item in tar:
                    tar.extract(item, f'{directory_path}/')
                dbutils.fs.rm(file_path.replace('/dbfs', ''))

                spark.sql(f"INSERT INTO {table_name} (event_name, event_id, starttime) VALUES ('{event_name}', {event_id}, current_timestamp())")
                
                break  # Break out of the loop if successful
            else:
                # Print an error message if the request was not successful
                print(f"Error (Attempt {attempt + 1}/{max_retries + 1}): {response.status_code} - {response.text}")

        except requests.exceptions.RequestException as e:
            # Handle exceptions (e.g., network errors)
            print(f"Request failed (Attempt {attempt + 1}/{max_retries + 1}): {e}")

            # Wait for a moment before the next attempt (optional)
            time.sleep(1)

        if attempt == max_retries:
            # If all attempts fail, raise an error
            # raise RuntimeError(f"API request failed after {max_retries + 1} attempts")
            print("Failed Response :" "\n url", url)
            parent_url.append(url)

# COMMAND ----------

# MAGIC %md ### Read Object id from API For Full Load

# COMMAND ----------

if direct_file_source_extraction_method == 'FULL':
    smresulted_df = spark.table("prod_bronze.pel_osc.osc_fattach")\
    .filter(
        (F.col("tbl").isin([1,2,3,9,106]))&
        (F.col("file_id").cast("int") >= F.lit(int(start_range))) &  (F.col("file_id").cast("int") <= F.lit(int(end_range)))
    )\
    .groupBy("id", "tbl_desc")\
    .count()\
    .withColumnRenamed("count", "No_of_Attachments")\
    .withColumnRenamed("id", "PrimaryID")\
    .withColumnRenamed("tbl_desc", "TableName")\
    .orderBy("No_of_Attachments", ascending=False)

    distinct_event_ids_df = spark.sql(f"SELECT DISTINCT event_id FROM {table_name}")
    distinct_event_ids_list = [row['event_id'] for row in distinct_event_ids_df.collect()]
    attachresulted_df = smresulted_df.filter(~smresulted_df.PrimaryID.isin(distinct_event_ids_list))

    smresulted_df.cache()
    attachresulted_df.cache()
    print(smresulted_df.count(),'\n',attachresulted_df.count())

if direct_file_source_extraction_method == 'DELTA':
    smresulted_df = spark.table("prod_bronze.pel_osc.osc_fattach")\
    .filter(
        (F.col("tbl").isin([1,2,3,9,106])) &
        (F.col("updated") >= F.lit(start_range)) &
        (F.col("updated") <= F.lit(end_range))
    )\
    .groupBy("id", "tbl_desc")\
    .count()\
    .withColumnRenamed("count", "No_of_Attachments")\
    .withColumnRenamed("id", "PrimaryID")\
    .withColumnRenamed("tbl_desc", "TableName")\
    .orderBy("No_of_Attachments", ascending=False)

    distinct_event_ids_df = spark.sql(f"SELECT DISTINCT event_id FROM {table_name}")
    distinct_event_ids_list = [row['event_id'] for row in distinct_event_ids_df.collect()]
    attachresulted_df = smresulted_df.filter(~smresulted_df.PrimaryID.isin(distinct_event_ids_list))

    smresulted_df.cache()
    attachresulted_df.cache()
    print(smresulted_df.count(),'\n',attachresulted_df.count())

# COMMAND ----------

# Assuming attachresulted_df is already defined and available
df_url = attachresulted_df \
    .withColumn('TableName', F.lower(F.col('TableName'))) \
    .withColumn('TableName', F.when(F.col('TableName') == 'events', 'incidents').otherwise(F.col('TableName'))) \
    .withColumn("url", F.concat(F.lit("https://pella.custhelp.com/services/rest/connect/latest/"), F.col('TableName'), F.lit("/"), F.col('PrimaryID'), F.lit("/fileAttachments?download")))

def download_files(url):
    try:
        return extract_files_with_retry(url)
    except Exception as e:
        print(f"Error downloading file from URL: {url}")
        print(f"Error message: {str(e)}")
        return None

def process_urls_in_batches(df, batch_size=1000):
    # Calculate the number of batches needed
    total_rows = df.count()
    num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
    
    for batch_num in range(num_batches):
        start_row = batch_num * batch_size
        end_row = start_row + batch_size
        batch_df = df.orderBy("PrimaryID").limit(end_row).tail(batch_size)
        
        # Convert DataFrame to list of URLs
        urls = [row['url'] for row in batch_df]
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(download_files, urls))
            # Process results here or accumulate them

# Call the function to process URLs in batches
process_urls_in_batches(df_url)

# COMMAND ----------

if direct_file_source_extraction_method == 'DELTA':
    attachresulted_df.unpersist()
if direct_file_source_extraction_method == 'FULL':
    smresulted_df.unpersist()
    attachresulted_df.unpersist()
