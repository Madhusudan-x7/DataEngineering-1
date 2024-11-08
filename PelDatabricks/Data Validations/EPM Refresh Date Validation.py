# Databricks notebook source
import pyspark
import requests
from delta.tables import *
from datetime import datetime, date
from datetime import timedelta
from pyspark.sql.functions import when,lit
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType,BooleanType,DateType,StructType,StructField,StringType,TimestampType
from pyspark.sql import SparkSession
from six.moves import urllib

# COMMAND ----------

# MAGIC %run "./EmailUtility"

# COMMAND ----------

dbutils.widgets.text("Schema","")
dbutils.widgets.text("TableCheckSchema","")
dbutils.widgets.text("ContainerName","")
dbutils.widgets.text("StorageAccountName","")
dbutils.widgets.text("DatalakeRefreshTable","")
dbutils.widgets.text("TablePath","")
dbutils.widgets.text("AuditEmailGroup","")

# COMMAND ----------

schema= dbutils.widgets.get('Schema')
table_check_schema= dbutils.widgets.get('TableCheckSchema')
container_name= dbutils.widgets.get('ContainerName')
storage_account_name= dbutils.widgets.get('StorageAccountName')
datalake_refresh_table= dbutils.widgets.get('DatalakeRefreshTable')
table_path= dbutils.widgets.get('TablePath')
audit_email_group= dbutils.widgets.get('AuditEmailGroup')
audit_email_group=list(audit_email_group.split(","))
print(audit_email_group)

# COMMAND ----------

#Creates the email message with a table including the list of tables that failed to refresh and all associated data.
def send_business_rules_msg():
    
    custom_html_msg=""
    body = "<html><head><style>table {font-family: Arial, Helvetica, sans-serif;border-collapse: collapse;width: 100%;}table td, table th {border: 1px solid #ddd;padding: 8px;}table tr:nth-child(even){background-color: #f2f2f2;}table th {padding-top: 12px;padding-bottom: 12px;text-align: left;background-color: #eee8aa;}</style></head>"
    str_table = "Dear User, <br> <br>Please find the results of the Datalake refresh results.<br><br><table><tr><th>PipelineTriggerName</th><th>TableName</th><th>SourceDateField</th><th>INS_DTM</th><th>TargetDTM</th><th>Result</th><th>Trigger</th><th>RefreshFrequency</th><th>EvaluationDate</th></tr>"
    str_table = body+str_table
    
    #Steps through each row in the data frame to first find if the evaluation is false, then creates a new row in the table to be displayed in the email.
    for row in evaluation_df.collect():
        if str(row["Result"]) == "false":
            trigger_name=str(row["PipelineTriggerName"])
            table_name=str(row["TableName"])
            source_date = str(row["SourceDateField"])
            ins_dtm = str(row["INS_DTM"])
            target_dtm = str(row["TargetDTM"])
            result = str(row["Result"])
            trigger = str(row["Trigger"])
            refresh = str(row["RefreshFrequency"])
            evaluationdate = str(row["EvaluationDate"])
            str_RW =   "<tr><td>" + trigger_name + "</td><td>" + table_name + "</td><td>" + source_date + "</td><td>" + ins_dtm + "</td><td>" + target_dtm + "</td><td>"  + result + "</td><td>"  + trigger + "</td><td>" + refresh +  "</td><td>" + evaluationdate +  "</td></tr>"
            str_table = str_table+str_RW
 
    str_table = str_table+"</table><br>Thank You,<br>Pella Corporation</html>"
    
    custom_html_msg=str_table
    return custom_html_msg

# COMMAND ----------

# MAGIC %pip install fsspec
# MAGIC #Set the format check to false so we can pull a csv file from the adls.
# MAGIC spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
# MAGIC
# MAGIC #Setting the table path and table name to be used in commands for pulling the tables to be checked and storing the results in a silver table.
# MAGIC datalake_refresh_tablepath= f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{table_path}"
# MAGIC abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/edw_files/EDW_Datalake_Refresh.csv"
# MAGIC #Creating the data frame from the stored csv in the dbfs.
# MAGIC evaluation_df=spark.read.option("header",True).csv(abfss_path)
# MAGIC #Writing the data frame to a delta table and creating the silver table from that stored delta table.
# MAGIC evaluation_df.write.format('delta').mode('overwrite').option("mergeSchema", "true").option("path", datalake_refresh_tablepath).saveAsTable(datalake_refresh_table)
# MAGIC
# MAGIC #Creating a data frame from the created silver table to be evaluated.
# MAGIC evaluation_df=spark.sql('select * from ' + datalake_refresh_table + '')
# MAGIC display(evaluation_df)
# MAGIC #Setting parameters for evaluations of the datalake tables.
# MAGIC today = date.today()
# MAGIC weekday = date.today().weekday()
# MAGIC false_result = False
# MAGIC
# MAGIC #Stepping through each datalake table to check when they were last refreshed.
# MAGIC for row in evaluation_df.collect():
# MAGIC     
# MAGIC     #Setting table and refresh attributes from the current row
# MAGIC     table_name = row['TableName']
# MAGIC     sourceDateName = row['SourceDateField']
# MAGIC     trigger = row['Trigger']
# MAGIC     refresh = row['RefreshFrequency']
# MAGIC     print(f"Table Check - {table_check_schema}.{table_name}")
# MAGIC     
# MAGIC     #Setting the select statements for the row based on the tables availability of an insert date field.
# MAGIC     if sourceDateName == "n/a":
# MAGIC         select_statement = "DESCRIBE HISTORY " + table_check_schema + "." + table_name
# MAGIC     else:
# MAGIC         select_statement = "SELECT MAX(" + sourceDateName + ") AS INS_DTM FROM " + table_check_schema + "." + table_name
# MAGIC     
# MAGIC     #Running the select statement to get the latest update for that table.
# MAGIC     df = spark.sql(select_statement)
# MAGIC     for date in df.collect():
# MAGIC         if sourceDateName == "n/a":
# MAGIC             if date['operation'] == 'WRITE':
# MAGIC                 ins_dtm = date['timestamp']
# MAGIC                 break
# MAGIC             else:
# MAGIC                 ins_dtm = today - timedelta(days=365)
# MAGIC         elif sourceDateName == "CURRENT_DTM_SKEY":
# MAGIC             current_DTM = str(date["INS_DTM"])
# MAGIC             year = current_DTM[0] + current_DTM[1] + current_DTM[2] + current_DTM[3]
# MAGIC             month = current_DTM[4] + current_DTM[5]
# MAGIC             day = current_DTM[6] + current_DTM[7]
# MAGIC             ins_dtm = datetime(int(year), int(month), int(day), 00, 00)
# MAGIC         else:
# MAGIC             ins_dtm = date['INS_DTM']
# MAGIC     
# MAGIC     #Stepping through the tables refresh trigger frequency to determine the latest refresh target date to compare against.
# MAGIC     #After finding the proper refresh target date, within the if statement is an evaluation of the latest refresh date and update of the silver table to store the results.
# MAGIC     if trigger == 'not scheduled':
# MAGIC         spark.sql("UPDATE " + datalake_refresh_table + " SET INS_DTM = 'n/a', TargetDTM = 'n/a', Result = False WHERE TableName = '" + table_name + "'")
# MAGIC         false_result = True
# MAGIC
# MAGIC     elif  trigger == 'Sunday 2 PM' or trigger == 'Saturday 10 AM':
# MAGIC         compare_date = today - timedelta(days=7)
# MAGIC
# MAGIC         if(compare_date <= ins_dtm.date()):
# MAGIC             spark.sql("UPDATE " + datalake_refresh_table + " SET INS_DTM = '" + str(ins_dtm.date()) + "', TargetDTM = '" + str(compare_date) + "', Result = True WHERE TableName = '" + table_name + "'")
# MAGIC         else:
# MAGIC             spark.sql("UPDATE " + datalake_refresh_table + " SET INS_DTM = '" + str(ins_dtm.date()) + "', TargetDTM = '" + str(compare_date) + "', Result = False WHERE TableName = '" + table_name + "'")
# MAGIC             false_result = True
# MAGIC
# MAGIC     elif trigger == 'Everyday 10 am' or trigger == 'Everyday 11 am' or trigger == 'Sunday - Friday 9 AM' or trigger == 'Sunday - Friday 7 AM':
# MAGIC         if refresh == "Daily":
# MAGIC             if weekday == 6 and (trigger == 'Sunday - Friday 9 AM' or trigger == 'Sunday - Friday 7 AM'):
# MAGIC                 compare_date = today - timedelta(days=1)
# MAGIC             else: 
# MAGIC                 compare_date = today
# MAGIC         elif refresh == "Last Business day":
# MAGIC             if weekday == 6:
# MAGIC                 compare_date = today - timedelta(days=2)
# MAGIC             elif weekday == 0:
# MAGIC                 compare_date = today - timedelta(days=3)
# MAGIC             else: 
# MAGIC                 compare_date = today - timedelta(days=1)
# MAGIC         else: 
# MAGIC             compare_date = today - timedelta(days=4)
# MAGIC
# MAGIC         if(compare_date <= ins_dtm.date()):
# MAGIC             spark.sql("UPDATE " + datalake_refresh_table + " SET INS_DTM = '" + str(ins_dtm.date()) + "', TargetDTM = '" + str(compare_date) + "', Result = True WHERE TableName = '" + table_name + "'")
# MAGIC         else:
# MAGIC             spark.sql("UPDATE " + datalake_refresh_table + " SET INS_DTM = '" + str(ins_dtm.date()) + "', TargetDTM = '" + str(compare_date) + "', Result = False WHERE TableName = '" + table_name + "'")
# MAGIC             false_result = True
# MAGIC
# MAGIC     else:
# MAGIC         spark.sql("UPDATE " + datalake_refresh_table + " SET INS_DTM = '" + str(ins_dtm.date()) + "', TargetDTM = 'n/a', Result = False WHERE TableName = '" + table_name + "'")
# MAGIC         false_result = True
# MAGIC
# MAGIC #pull the refresh results from the silver table to create a data frame with the results and set the evaluation date.
# MAGIC evaluation_df=spark.sql('select * from ' + datalake_refresh_table + ' order by ID')
# MAGIC evaluation_df = evaluation_df.withColumn("EvaluationDate",current_timestamp())
# MAGIC display(evaluation_df)
# MAGIC
# MAGIC #Use the created data frame to create an email message with the failed tables.
# MAGIC datalake_refresh_msg=send_business_rules_msg()
# MAGIC
# MAGIC #Check the false_result field to see if there were any failed tables to refresh 
# MAGIC if false_result == True:
# MAGIC     print('Refresh Failed')
# MAGIC     send_mail(send_from = "db@pella.com", send_to = audit_email_group, subject = "Notification Alert! - Datalake Refresh Failure", message = datalake_refresh_msg, isHTMLMsg=True, files=None, server="mail.pella.com", port=25, use_tls=True)
# MAGIC else:
# MAGIC     #Print that the refresh succeeded and do not send an email.
# MAGIC     print('Refresh Succeeded')

# COMMAND ----------


