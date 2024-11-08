# Databricks notebook source
import pyspark
import requests
import datetime
from delta.tables import *
from datetime import datetime, date, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from six.moves import urllib

# COMMAND ----------

# MAGIC %run "./EmailUtility"

# COMMAND ----------

dbutils.widgets.text("DataLakeDestinationContainer","")
dbutils.widgets.text("StorageAccountName","")
dbutils.widgets.text("ProfiseeEmailGroup","")

# COMMAND ----------

DataLakeDestinationContainer= dbutils.widgets.get('DataLakeDestinationContainer')
StorageAccountName= dbutils.widgets.get('StorageAccountName')
audit_email_group= dbutils.widgets.get('ProfiseeEmailGroup')
audit_email_group=list(audit_email_group.split(","))
print(audit_email_group)

# COMMAND ----------

import datetime
from datetime import datetime, date, timezone

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

url_response_tablepath = f"abfss://{StorageAccountName}@{DataLakeDestinationContainer}.dfs.core.windows.net/user/schlatterch/URL_Response.csv"
url_csv_df=spark.read.format("csv").option("delimiter", ",").option("header",True).load(url_response_tablepath)
display(url_csv_df)

ft = "%Y-%m-%d T%H:%M:%S"
utc_dt = datetime.now(timezone.utc) # UTC time
time = utc_dt.astimezone().strftime(ft)
final_result = True
custom_html_msg=""
body = "<html><head><style>table {font-family: Arial, Helvetica, sans-serif;border-collapse: collapse;width: 100%;}table td, table th {border: 1px solid #ddd;padding: 8px;}table tr:nth-child(even){background-color: #f2f2f2;}table th {padding-top: 12px;padding-bottom: 12px;text-align: left;background-color: #eee8aa;}</style></head>"
str_table = f"Dear User, <br> <br>Please find the results of the URL response checks as of {time}.<br><br><table><tr><th>Environment</th><th>URL</th><th>Status_Code</th><th>Result</th></tr>"
str_table = body+str_table

for url in url_csv_df.collect():
    urlstring = str(url["URL"])
    status = str(urllib.request.urlopen(urlstring).getcode())
    
    if status == '200':
        result = 'True'
    else:
        result = 'False'
        final_result = False

    environment = str(url["Environment"])
    str_RW =   "<tr><td>" + environment + "</td><td>" + urlstring + "</td><td>" + status + "</td><td>" + result + "</td></tr>"

    str_table = str_table+str_RW

str_table = str_table+"</table><br>Thank You,<br>Pella Corporation</html>"

custom_html_msg=str_table

if final_result == False:
    print('Response Failed')
    send_mail(send_from = "db@pella.com", send_to = audit_email_group, subject = "Notification Alert! - URL Response Check Failure", message = custom_html_msg, isHTMLMsg=True, files=None, server="mail.pella.com", port=25, use_tls=True)
else:
    print('Response Succeeded')

# COMMAND ----------


