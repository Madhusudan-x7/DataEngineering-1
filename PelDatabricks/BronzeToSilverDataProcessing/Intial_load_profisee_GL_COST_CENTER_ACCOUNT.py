# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql.functions import to_json, struct,col
import json
import requests

# COMMAND ----------

# DBTITLE 1,Define Parameter list
dbutils.widgets.text("ProfiseeHostname","")
dbutils.widgets.text("ProfiseeDatabase","")
dbutils.widgets.text("ProfiseeUsername","")
dbutils.widgets.text("ProfiseePasswordKey","")
dbutils.widgets.text("DatabricksScope","")
dbutils.widgets.text("ProfiseeAPIKey","")
dbutils.widgets.text("ProfiseeHostURL","")



# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get Parameters    

profiseeHostname=dbutils.widgets.get('ProfiseeHostname')
profiseeDatabase=dbutils.widgets.get('ProfiseeDatabase')
profiseeUsername=dbutils.widgets.get('ProfiseeUsername')
profiseePasswordKey=dbutils.widgets.get('ProfiseePasswordKey')
databricksScope=dbutils.widgets.get('DatabricksScope')
profiseeAPIKey=dbutils.widgets.get('ProfiseeAPIKey')
profiseeHostURL=dbutils.widgets.get('ProfiseeHostURL')

# COMMAND ----------

# DBTITLE 1,Connect to Profisee
profiseePort = 1433
profiseePassword = dbutils.secrets.get(scope=databricksScope,key=profiseePasswordKey) # Replace the scope and key accordingly
profiseeAPIKeyVal=dbutils.secrets.get(scope=databricksScope,key=profiseeAPIKey)

# Create profisee URL
profiseeUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(profiseeHostname, profiseePort, profiseeDatabase)
print(profiseeUrl)
profiseeConnectionProperties = {
  "user" : profiseeUsername,
  "password" : profiseePassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# DBTITLE 1,Read from Profisee (JDBC)
#read table data into a spark dataframe
profiseeDf = spark.read.format("jdbc") \
    .option("url", profiseeUrl) \
    .option("query", "Select Data_Source as Data_source, Code as code, Source_ID as source_id ,Segment1,Segment2,Segment3,Location, CostCenter from  data.vGL_Cost_Center") \
    .option("user", profiseeUsername) \
    .option("password", profiseePassword) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
 
#show the data loaded into dataframe
display(profiseeDf)

# COMMAND ----------

# DBTITLE 1,Get Source Data
costcenterDf = spark.sql( """select distinct  Name ,Data_Source ,source_id, Segment1 ,Segment2,Segment3,Segment2 Location,COST_CENTER_DESC CostCenter   from silver.edwproddb_EDWPROD_EDW_EXT_PROFISEE_COST_CENTER_V""")

costcenterDf.head(1)

# COMMAND ----------

# DBTITLE 1,Get Reference Data
referenceDf = spark.read.format("jdbc") \
    .option("url", profiseeUrl) \
    .option("query", "select Data_Source as Data_source, Code as code,Location_Description as Location,Segment1,Segment2 from data.vLocation") \
    .option("user", profiseeUsername) \
    .option("password", profiseePassword) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
 
#show the data loaded into dataframe
display(referenceDf)

# COMMAND ----------

# DBTITLE 1,Get Reference Code
costcenterDf = costcenterDf.join(referenceDf,["Segment1","Segment2"],"inner") \
                  .select(col("Name"),costcenterDf["Data_Source"],col("source_id"),costcenterDf["Segment1"],costcenterDf["Segment2"],col("Segment3") \
                      ,col("CostCenter"),referenceDf["code"].alias("Location") \
                      )
             

display(costcenterDf)

# COMMAND ----------

# DBTITLE 1,Create temp view
costcenterDf.createOrReplaceTempView("costcenter")
profiseeDf.createOrReplaceTempView("Profisee")

# COMMAND ----------

# DBTITLE 1,Hash logic
costcenterDf=spark.sql(""" Select *, md5(concat_ws(Segment1,Segment2,Segment3,Location,CostCenter)) as hash_key_C from costcenter""")
profiseeDf=spark.sql(""" Select *, md5(concat_ws(Segment1,Segment2,Segment3,Location,CostCenter)) as hash_key_P from Profisee""")
costcenterDf.createOrReplaceTempView("costcenter_final")
profiseeDf.createOrReplaceTempView("Profisee_final")

# COMMAND ----------

# DBTITLE 1,Create Dataframe to Insert
InsertDf=spark.sql(""" SELECT c.Name, c.Data_Source, c.source_id, c.Segment1, c.Segment2, c.Segment3,c.Location,c.CostCenter FROM costcenter_final c LEFT ANTI JOIN Profisee_final p ON c.Data_Source == p.Data_Source and c.source_id == p.source_id""")
print(InsertDf.count())

# COMMAND ----------

# DBTITLE 1,Create Dataframe to update
UpdateDF=spark.sql(""" SELECT c.Name, c.Data_Source, c.source_id, c.Segment1, c.Segment2, c.Segment3, p.Location,c.CostCenter,p.code FROM costcenter_final as c LEFT JOIN Profisee_final p ON c.Data_Source == p.Data_Source and c.source_id == p.source_id where c.hash_key_C  !=p.hash_key_P""")
print(UpdateDF.count())

# COMMAND ----------

# DBTITLE 1,Convert Update DF to json array
df_update=UpdateDF.toJSON().collect()
def listToString(s):
    str1 = "," 
    return (str1.join(s))

Updatepayload=listToString(df_update)
Updatepayloadfinal = '['+Updatepayload+']'
print(Updatepayloadfinal)

# COMMAND ----------

# DBTITLE 1,Update RestApi request
#Need to modify it to a patch request

url = f"""https://{profiseeHostURL}/Profisee/rest/v1/Records/GL_Cost_Center?IsUpsert=true"""
headers = {"Accept":"application/json -H","x-api-key":profiseeAPIKeyVal,"Content-Type": "application/json-patch+json"} 
response = requests.patch(url, headers=headers, data=Updatepayloadfinal)
print(response)

# COMMAND ----------

# DBTITLE 1,Convert Insert DF to json array
df_insert=InsertDf.toJSON().collect()
def listToString(s):
    str1 = "," 
    return (str1.join(s))

Insertpayload=listToString(df_insert)
Insertpayloadfinal = '['+Insertpayload+']'
print(Insertpayloadfinal)

# COMMAND ----------

# DBTITLE 1,Insert Restpi request
url = f"""https://{profiseeHostURL}/Profisee/rest/v1/Records/GL_Cost_Center"""
headers = {"Accept":"application/json -H","x-api-key":profiseeAPIKeyVal,"Content-Type": "application/json-patch+json"} 
response = requests.post(url, headers=headers, data=Insertpayloadfinal)
