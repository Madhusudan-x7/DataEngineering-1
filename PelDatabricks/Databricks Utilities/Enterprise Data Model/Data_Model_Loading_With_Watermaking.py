# Databricks notebook source
dbutils.widgets.text("silver_catalog", "")
dbutils.widgets.text("bronze_catalog", "")

# Get parameters from job
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("workspace_url", "")
dbutils.widgets.text("workspace_id", "")

# COMMAND ----------

silver_catalog = dbutils.widgets.get("silver_catalog")
bronze_catalog = dbutils.widgets.get("bronze_catalog")

# Get parameters from job
job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")
workspace_url= dbutils.widgets.get("workspace_url")
workspace_id= dbutils.widgets.get("workspace_id")


# COMMAND ----------

# DBTITLE 1,Importing necessary libraries
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
import datetime, re

# COMMAND ----------

#working code with table tag validation

def read_query_and_merge_or_create(source_query: str, target_table: str, merge_condition: str) -> None:
    """
    Reads data using a provided SQL query, then merges into an existing Delta table or creates a new one if it doesn't exist.
    
    :param source_query: SQL query to read source data.
    :param target_table: Full name of the target table (including catalog and schema).
    :param merge_condition: Condition on which merge is based.
    """
    # Read data using the provided query :: Assupmtion: insert and upd time in target table

    source_df = spark.sql(source_query)
    source_df = source_df.withColumn("ins_dtm_utc", current_timestamp()) \
                 .withColumn("upd_dtm_utc", current_timestamp())
   

    # update_mapping = {col: f"source.{col}" for col in source_df.columns if col not in exclude_update_columns}
    update_mapping = {col: f"source.{col}" for col in source_df.columns if col != target_table_skey and col != "ins_dtm_utc"}
    insert_mapping = {col: f"source.{col}" for col in source_df.columns if col != target_table_skey }

    # Perform merge operation
    deltaTable = DeltaTable.forName(spark, target_table)
    history_before = deltaTable.history(1).select("version").collect()[0]["version"]

    try:
        deltaTable.alias("target") \
                  .merge(
                      source_df.alias("source"),
                      merge_condition
                  ) \
                  .whenMatchedUpdate(set=update_mapping) \
                  .whenNotMatchedInsert(values=insert_mapping) \
                  .execute()
        merge_status = f"Table {target_table} merged successfully."
       
    except Exception as e:
        merge_status = f"Error during merge operation: {str(e)}"
        # print(merge_status)
    history_after = deltaTable.history(1).select("version").collect()[0]["version"]
    if history_before != history_after:
        merge_status += f" Version updated from {history_before} to {history_after}."
        pl_last_run_dtm_utc= datetime.datetime.now()
        spark.sql(f"""UPDATE {silver_catalog}.edm_control.edm_generic_load 
                  SET pl_last_run_dtm_utc = current_timestamp(), 
                  pl_last_status = '{merge_status}' WHERE ctl_tbl_id = '{ctl_tbl_id}'""")       
    else:
        merge_status += " No changes made."
        spark.sql(f"""UPDATE {silver_catalog}.edm_control.edm_generic_load 
                  SET pl_last_run_dtm_utc = current_timestamp(), 
                  pl_last_status = '{merge_status}' WHERE ctl_tbl_id = '{ctl_tbl_id}'""")
        

# COMMAND ----------

#working code with table tag validation

from pyspark.sql.functions import monotonically_increasing_id, expr, when, current_timestamp, max, lit, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr

# Fetch data from control table
control_data = spark.sql(f"SELECT * FROM {silver_catalog}.edm_control.edm_generic_load where enabled= true order by tgt_load_seq")

# Iterate over control table data
for row in control_data.collect():
    op_data_src=row["op_data_src"]
    ctl_tbl_id=row["ctl_tbl_id"]
    src_tbl_wm_col = row["src_tbl_wm_col"]

    #updating src qury based on watermark column if exist
    if src_tbl_wm_col is None:
        source_query = row["src_qry"].format(op_data_src=op_data_src,bronze_catalog=bronze_catalog, silver_catalog=silver_catalog)

    else:
        last_upd_wm_dtm_utc = row["last_upd_wm_dtm_utc"]
        source_query = row["src_qry"].format(bronze_catalog=bronze_catalog)
        base_table = source_query[source_query.upper().find("FROM"):].split()[1].strip()
        max_wm_col=spark.sql(f"select max({src_tbl_wm_col}) as max_wm from {base_table}").collect()[0]["max_wm"]
        source_query = f"{row['src_qry']} WHERE {src_tbl_wm_col} > '{last_upd_wm_dtm_utc}' AND {src_tbl_wm_col} <= '{max_wm_col}'"
        
    print(row["ctl_tbl_id"])
    target_table=row["tgt_tbl"].format(silver_catalog=silver_catalog)
    base_table=target_table.split(".")[2]
    print(base_table)
    schema_name=target_table.split(".")[1]
    print(schema_name)
    model_table_version = spark.sql(f"""SELECT tag_value as version
                   FROM system.information_schema.table_tags 
                   WHERE catalog_name = '{silver_catalog}' and table_name = '{base_table}' and schema_name='{schema_name}' and tag_name='model_table_version'""").collect()[0]["version"]
    
    
    op_data_src=row["op_data_src"]
    merge_key= row["merge_key"].split(",") 
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_key])
    target_table_skey=row["tgt_tbl_id_col"]
    # exclude_update_columns = row["exclude_upd_col"]   #new column to be added in conrol table
    
    #constructing job url
    domain = re.search(r"https?://([^/?]+)", workspace_url).group(1)
    job_url= f"https://{domain}/jobs/{job_id}/runs/{run_id}?o={workspace_id}"
    displayHTML(f'<a href="{job_url}" target="_blank">{job_url}</a>')
    
    if model_table_version==row["tbl_ver"]: 

         #function to merge data into target table
         read_query_and_merge_or_create(source_query, target_table, merge_condition)
     
         #updating control table watermark values - 
         if src_tbl_wm_col is None:
             spark.sql(f"update {silver_catalog}.edm_control.edm_generic_load set last_run_nbk = '{job_url}' WHERE ctl_tbl_id = '{ctl_tbl_id}'")
         else:
             spark.sql(f"update {silver_catalog}.edm_control.edm_generic_load set last_upd_wm_dtm_utc = '{max_wm_col}', last_run_nbk = '{job_url}' WHERE ctl_tbl_id = '{ctl_tbl_id}'")
    else:
        merge_status = f"Table version mismatch. Table version in control table is {row['tbl_ver']} and table version from model is {model_table_version}"
        spark.sql(f"update {silver_catalog}.edm_control.edm_generic_load set last_run_nbk = '{job_url}', pl_last_run_dtm_utc = current_timestamp(), pl_last_status='{merge_status}'  WHERE ctl_tbl_id = '{ctl_tbl_id}'")

