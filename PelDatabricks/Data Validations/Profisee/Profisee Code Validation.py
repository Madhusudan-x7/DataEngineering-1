# Databricks notebook source
import pyspark
import pandas as pd
from delta.tables import *
from datetime import datetime, date
from datetime import timedelta
from pyspark.sql.functions import when,lit
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType,BooleanType,DateType,StructType,StructField,StringType,TimestampType
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run "../EmailUtility"

# COMMAND ----------

dbutils.widgets.text("Schema","")
dbutils.widgets.text("TableCheckSchema","")
dbutils.widgets.text("ContainerName","")
dbutils.widgets.text("StorageAccountName","")
dbutils.widgets.text("ProfiseeCodeTable","")
dbutils.widgets.text("TablePath","")
#Adding an email group widget
dbutils.widgets.text("ProfiseeCodeEmailGroup","")

# COMMAND ----------

schema= dbutils.widgets.get('Schema')
table_check_schema= dbutils.widgets.get('TableCheckSchema')
container_name= dbutils.widgets.get('ContainerName')
storage_account_name= dbutils.widgets.get('StorageAccountName')
profisee_code_table= dbutils.widgets.get('ProfiseeCodeTable')
table_path= dbutils.widgets.get('TablePath')
#Getting the email group value
profisee_email_group= dbutils.widgets.get('ProfiseeCodeEmailGroup')
profisee_email_group=list(profisee_email_group.split(","))
print(profisee_email_group)

# COMMAND ----------

# MAGIC %pip install fsspec
# MAGIC %pip install adlfs
# MAGIC #Set the format check to false so we can pull a csv file from the adls.
# MAGIC spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
# MAGIC
# MAGIC #Setting the table path, table name and path parameters to be used in commands for pulling the tables to be checked and storing the results in a silver table.
# MAGIC profisee_code_tablepath= f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{table_path}"
# MAGIC abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/edw_files/Profisee_Code_Validation.csv"
# MAGIC #Creating the data frame from the stored csv in the dbfs.
# MAGIC evaluation_df=spark.read.option("header",True).csv(abfss_path)
# MAGIC #Writing the data frame to a delta table and creating the silver table from that stored delta table.
# MAGIC evaluation_df.write.format('delta').mode('overwrite').option("mergeSchema", "true").option("path", profisee_code_tablepath).saveAsTable(profisee_code_table)
# MAGIC
# MAGIC #Creating a data frame from the created silver table to be evaluated.
# MAGIC evaluation_df=spark.sql('select * from ' + profisee_code_table + '')
# MAGIC display(evaluation_df)
# MAGIC
# MAGIC current_period = spark.sql('SELECT CURRENT_YEAR, FIRST_DAY_CY_DTM_SKEY FROM ' + schema + '.pel_edwprod.edwadmin_edw_current_period_v')
# MAGIC for period in current_period.collect():
# MAGIC     current_year = int(period["CURRENT_YEAR"])
# MAGIC     current_year = "FY" + str(current_year)
# MAGIC     first_day_DTM = str(period["FIRST_DAY_CY_DTM_SKEY"])
# MAGIC     year = first_day_DTM[0] + first_day_DTM[1] + first_day_DTM[2] + first_day_DTM[3]
# MAGIC     month = first_day_DTM[4] + first_day_DTM[5]
# MAGIC     day = first_day_DTM[6] + first_day_DTM[7]
# MAGIC     first_day_FY = datetime(int(year), int(month), int(day), 00, 00)
# MAGIC
# MAGIC #Stepping through each table to check the Profisee codes.
# MAGIC for row in evaluation_df.collect():
# MAGIC
# MAGIC     #Setting table and refresh attributes from the current row
# MAGIC     table_name = row['TableName']
# MAGIC     print(table_name)
# MAGIC     columns = row['ColumnsToCheck']
# MAGIC     fy_column = row['FYColumn']
# MAGIC
# MAGIC     column_list=list(columns.split("|"))
# MAGIC     column_checks = column_list[0] + " = 'NO CODE FOUND'"
# MAGIC     
# MAGIC     for i in range(len(column_list)):
# MAGIC         if i == 0:
# MAGIC             continue
# MAGIC         else:
# MAGIC             column_checks = column_checks + " OR " + column_list[i] + " = 'NO CODE FOUND'"
# MAGIC
# MAGIC     if fy_column == 'ORDER_DTM':
# MAGIC         select_statement = "SELECT * FROM " + table_check_schema + "." + table_name + " WHERE " + fy_column + " >= '" + str(first_day_FY) + "' AND SALES_CHANNEL <> 'NON DISTRIBUTOR' AND PLANT_CODE <> 'WINDOR' AND (" + column_checks + ")"
# MAGIC     elif fy_column == 'FISCAL_YEAR':
# MAGIC         select_statement = "SELECT * FROM " + table_check_schema + "." + table_name + " WHERE " + fy_column + " >= '" + current_year + "' AND (" + column_checks + ")"
# MAGIC     
# MAGIC     print(select_statement)
# MAGIC     
# MAGIC     df = spark.sql(select_statement)
# MAGIC     today = str(datetime.today())
# MAGIC     #write the data frame as a csv file to the adls profiseeStaging folder
# MAGIC     table_path = f"adl://{container_name}@{storage_account_name}.dfs.core.windows.net/profiseeStaging/{table_name}.csv"
# MAGIC     print(table_path,"\n")
# MAGIC     
# MAGIC     if df.count() > 0:
# MAGIC         display(df)
# MAGIC         df_results = df.toPandas()
# MAGIC         df_results.to_csv(table_path,index=True)
# MAGIC         spark.sql("UPDATE " + profisee_code_table + " SET Result = False, EvaluationDate = '" + today + "' WHERE TableName = '" + table_name + "'")
# MAGIC     else:
# MAGIC         spark.sql(f"UPDATE " + profisee_code_table + " SET Result = True, EvaluationDate = '" + today + "' WHERE TableName = '" + table_name + "'")

# COMMAND ----------

def send_business_rules_msg():
    
    custom_html_msg=""
    body = "<html><head><style>table {font-family: Arial, Helvetica, sans-serif;border-collapse: collapse;width: 100%;}table td, table th {border: 1px solid #ddd;padding: 8px;}table tr:nth-child(even){background-color: #f2f2f2;}table th {padding-top: 12px;padding-bottom: 12px;text-align: left;background-color: #eee8aa;}</style></head>"
    str_table = "Dear User, <br> <br>Please find the results of the failed Profisee code validation below.<br><br><table><tr><th>ID</th><th>TableName</th><th>FYColumn</th><th>ColumnsToCheck</th><th>Result</th><th>EvaluationDate</th></tr>"
    str_table = body+str_table
    
    #Steps through each row in the data frame to first find if the evaluation is false, then creates a new row in the table to be displayed in the email.
    for row in evaluation_df.collect():
        if str(row["Result"]) == "false":
            id=str(row["ID"])
            table_name=str(row["TableName"])
            fy_column=str(row["FYColumn"])
            columns = str(row["ColumnsToCheck"])
            result = str(row["Result"])
            evaluation_date = str(row["EvaluationDate"])
            str_RW =   "<tr><td>" + id + "</td><td>" + table_name + "</td><td>" + fy_column + "</td><td>" + columns + "</td><td>" + result + "</td><td>" + evaluation_date + "</td></tr>"
            str_table = str_table+str_RW
 
    str_table = str_table+"</table><br>Thank You,<br>Pella Corporation</html>"
    
    custom_html_msg=str_table
    return custom_html_msg

# COMMAND ----------

profisee_code_table= dbutils.widgets.get('ProfiseeCodeTable')
#pull the refresh results from the silver table to create a data frame with the results and set the evaluation date.
evaluation_df=spark.sql('select * from ' + profisee_code_table + ' order by ID')
display(evaluation_df)

#Check if any of the final results are False.
overall_result = True
for row in evaluation_df.collect():
    if str(row["Result"]) == "false":
        overall_result = False
        break
    else:
        continue

#Use the created data frame to create an email message with the failed tables.
profisee_code_validation_msg=send_business_rules_msg()

#Check the overall_result field to see if there were any failed tables to refresh 
print(overall_result)
if overall_result == False:
    print('Refresh Failed')
    
    #collect the csv files of the tables that had a null Profisee code to sent in an email.
    for row in evaluation_df.collect():
        if str(row["Result"]) == "false":
            table_name = str(row["TableName"])
            table_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/profiseeStaging/{table_name}.csv"
        
            #Sending an email with a list of the failed tables and separate csv attachments with the failed rows from each table.
            send_mail(send_from = "db@pella.com", send_to = profisee_email_group, subject = "Profisee Code Validation Failure - "+ table_name, message = profisee_code_validation_msg, isHTMLMsg=True, files=[table_path], server="mail.pella.com", port=25, use_tls=True)
        
else:
    #Print that the refresh succeeded and do not send an email.
    print('Refresh Succeeded')

# COMMAND ----------


