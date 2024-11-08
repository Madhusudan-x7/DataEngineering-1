# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql.functions import to_json, struct, md5, concat_ws, col
import json
import requests
import datetime, time

# COMMAND ----------

# DBTITLE 1,Define Parameter list
dbutils.widgets.text("pSourceExtractQuery","")
dbutils.widgets.text("pProfiseeEntityQuery","")
dbutils.widgets.text("pProfiseeHostname","")
dbutils.widgets.text("pProfiseePort","")
dbutils.widgets.text("pProfiseeDatabase","")
dbutils.widgets.text("pDatabricksScope","")
dbutils.widgets.text("pProfiseeAPIKey","")
dbutils.widgets.text("pProfiseePwd","")
dbutils.widgets.text("pProfiseeUsername","")
dbutils.widgets.text("pReferencedEntityQuery","")
dbutils.widgets.text("pProfiseeEntityName","")
dbutils.widgets.text("pSourceHashColumns","")
dbutils.widgets.text("pSourceEntityJoinColumns","")
dbutils.widgets.text("pReferencedEntityKeyColumns","")
dbutils.widgets.text("pReferencedEntityColAlias","")
dbutils.widgets.text("pSourceReferenceJoinColumns","")
dbutils.widgets.text("pProfiseeHostURL","")

profiseeHostUrl=dbutils.widgets.get("pProfiseeHostURL")
SourceExtractSQL = dbutils.widgets.get('pSourceExtractQuery')
EntityLkpQuery = dbutils.widgets.get('pProfiseeEntityQuery')
profiseeHostname = dbutils.widgets.get('pProfiseeHostname')
profiseePort = dbutils.widgets.get('pProfiseePort')
profiseeDatabase = dbutils.widgets.get('pProfiseeDatabase')
databricksScope = dbutils.widgets.get('pDatabricksScope')
profiseeAPIkey = dbutils.widgets.get('pProfiseeAPIKey')
profiseePwd = dbutils.widgets.get('pProfiseePwd')
profiseeUsername = dbutils.widgets.get('pProfiseeUsername')
ParentLkpQuery = dbutils.widgets.get('pReferencedEntityQuery')
ProfiseeEntity = dbutils.widgets.get('pProfiseeEntityName')
ParentEntitycol = dbutils.widgets.get('pReferencedEntityKeyColumns')
ParentEntitycolalias = dbutils.widgets.get('pReferencedEntityColAlias')
ColumnsHash = dbutils.widgets.get('pSourceHashColumns')
SrcPrntjoin = dbutils.widgets.get('pSourceReferenceJoinColumns')
SrcEntitycol= dbutils.widgets.get('pSourceEntityJoinColumns')

# COMMAND ----------

# DBTITLE 1,Define connection string
profiseePassword = dbutils.secrets.get(scope=databricksScope,key=profiseePwd) # Replace the scope and key accordingly
profiseeAPIPassword = dbutils.secrets.get(scope=databricksScope,key=profiseeAPIkey) # Replace the scope and key accordingly

# Create profisee URL
profiseeUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(profiseeHostname, profiseePort, profiseeDatabase)
print(profiseeUrl)
profiseeConnectionProperties = {
  "user" : profiseeUsername,
  "password" : profiseePassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# DBTITLE 1,Read from Delta Lake
SourceExtractSQL_Df = spark.sql(SourceExtractSQL)

#Remove the reference entitycol from source
if ParentEntitycolalias or ProfiseeEntity.lower() == "division":
    if ProfiseeEntity.lower() == "gl_cost_center" or ProfiseeEntity.lower() == "division" or ProfiseeEntity.lower() == "location":
        SourceExtractSQL_Df=SourceExtractSQL_Df.drop(ParentEntitycolalias)

SourceExtractSQL_Df.createOrReplaceTempView("source")

# COMMAND ----------

# DBTITLE 1,Read from Profisee (JDBC Entity)
#read table data into a spark dataframe
EntityLkp_df = spark.read.format("jdbc") \
     .option("url", profiseeUrl) \
     .option("query", EntityLkpQuery) \
     .option("user", profiseeUsername) \
     .option("password", profiseePassword) \
     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
     .load()

EntityLkp_df.createOrReplaceTempView("EntityLkp")

# COMMAND ----------

# DBTITLE 1,Read from Profisee (JDBC parent)
#read table data into a spark dataframe
#if HasParent.lower()=="yes":
if ParentLkpQuery:
    ParentLkp_Df = spark.read.format("jdbc") \
    .option("url", profiseeUrl) \
    .option("query", ParentLkpQuery) \
    .option("user", profiseeUsername) \
    .option("password", profiseePassword) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

    ParentLkp_Df.createOrReplaceTempView("ParentLkp")

# COMMAND ----------

# DBTITLE 1,Hash logic
key_column=list(ColumnsHash.split(","))
parent_col=list(ParentEntitycol.split(","))
arr=[]
for x in range(len(parent_col)):
    arr.insert(x, "parent." +parent_col[x])
seperator_ent= ","
par_col=seperator_ent.join(arr)

if ParentLkpQuery:
#if HasParent.lower()=="yes":
    
    join_col_src_ref=list(SrcPrntjoin.split(","))
    joinCriteria=[]
    for x in range(len(join_col_src_ref)):
        joinCriteria.insert(x, "src." +join_col_src_ref[x]+" = parent."+join_col_src_ref[x])
    seperatorJoin= " and "
    joinStmt=seperatorJoin.join(joinCriteria)
    print(joinStmt)
    
    SourceExtractSQL_Df=spark.sql("""Select src.*,{par_col} as {ParentEntitycolalias} from source src inner join ParentLkp parent on {joinStmt}""".format(par_col=par_col, ParentEntitycolalias=ParentEntitycolalias,joinStmt=joinStmt)) 
    
    display(SourceExtractSQL_Df)

SourceDf=SourceExtractSQL_Df.withColumn("hash_key", md5(concat_ws("",*key_column)))
    
SourceDf.createOrReplaceTempView("source_final");

# COMMAND ----------

# DBTITLE 1,Hash logic
Entity_df=EntityLkp_df.withColumn("hash_key", md5(concat_ws("",*key_column)))
Entity_df.createOrReplaceTempView("entity_final")

# COMMAND ----------

# DBTITLE 1,Generate join condition
join_col=list(SrcEntitycol.split(","))
joinCriteriaEntity=[]
for x in range(len(join_col)):
    joinCriteriaEntity.insert(x, "src." +join_col[x]+" = profisee_entity."+join_col[x])
seperator= " and "
p=seperator.join(joinCriteriaEntity)
print(p)

# COMMAND ----------

# DBTITLE 1,Create Dataframe to Insert
InsertDf=spark.sql(""" SELECT src.* FROM source_final src LEFT ANTI JOIN entity_final profisee_entity ON {p} """.format(p=p))
InsertDf=InsertDf.drop(col("hash_key"))
if ProfiseeEntity.lower()=='division':
    InsertDf=InsertDf.withColumnRenamed('Segment1','Segment 1').withColumnRenamed('Division','Division Description')

# COMMAND ----------

if ParentLkpQuery:
    refColumnInsert=ParentEntitycolalias
    refColumnInsert=refColumnInsert.replace("_REFERENCE","")
    print(refColumnInsert)

    InsertDf=InsertDf.drop(refColumnInsert)

    InsertDf=InsertDf.withColumnRenamed(ParentEntitycolalias,refColumnInsert)

# COMMAND ----------

# DBTITLE 1,Create Dataframe to update
parent_col=list(ParentEntitycol.split(","))
arr=[]
for x in range(len(parent_col)):
    arr.insert(x, "profisee_entity." +parent_col[x])
seperator_ent= ","
par_col=seperator_ent.join(arr)

update_query=f"SELECT src.*,{par_col} FROM source_final src inner JOIN entity_final profisee_entity  ON {p} where src.hash_key!=profisee_entity.hash_key"

UpdateDF=spark.sql(update_query)
UpdateDF=UpdateDF.drop(col("hash_key"))
if ProfiseeEntity.lower()=='division':
    UpdateDF=UpdateDF.withColumnRenamed('Segment1','Segment 1').withColumnRenamed('Division','Division Description')
elif ProfiseeEntity.lower()=='branch_account_number':
    UpdateDF=UpdateDF.drop('branch_account_location')

# COMMAND ----------

  #updateDataframe manipulation
if ParentLkpQuery:
    refColumn=ParentEntitycolalias
    refColumn=refColumn.replace("_REFERENCE","")
    print(refColumn)

    UpdateDF=UpdateDF.drop(refColumn)

    UpdateDF=UpdateDF.withColumnRenamed(ParentEntitycolalias,refColumn)

# COMMAND ----------

# update the records
print(UpdateDF.count())
display(UpdateDF)
update_numbr_splits = (UpdateDF.count()//10000)+1
update_each_len = 10000
update_copy_df = UpdateDF

if UpdateDF.count()>0:
    pandasDFUpdate = UpdateDF.toPandas()
  
    batch=0
    for i in range(0,update_numbr_splits ):
    
        update_temp_df=pandasDFUpdate.iloc[batch:batch+update_each_len]
        df_update=update_temp_df.to_json(orient='records')
    
        url = f"https://{profiseeHostUrl}/Profisee/rest/v1/Records/{ProfiseeEntity}?IsUpsert=true"
        headers = {"Accept":"application/json -H","x-api-key":profiseeAPIPassword,"Content-Type": "application/json-patch+json"} 

        response = requests.patch(url, headers=headers, data=df_update)
        
        httpCode=str(response.status_code)
        if response.status_code==201 or response.status_code==200:
            
            print("successfully updated")
            #logger.info("-Message :" + "Successfull_Update_For_Batch: " +df_update +"-http:"+httpCode)
    
        else:
            print("Not processed")
            #logger.error("-Message :" + "Error_Update_For_Batch: " +df_update +"-http:"+httpCode)
            raise ValueError("Failed to process the Batch Updates. Success response code not received from the API : " + httpCode)
        
        i=i+1
        batch=batch+update_each_len
        ##sendLogs(logs_storage_path,filename)
else:    
    print("No Records Found For Upserts")
    #sendLogs(logs_storage_path,filename)

# COMMAND ----------

insert_numbr_splits = (InsertDf.count()//10000)+1
insert_each_len = 10000

print(insert_numbr_splits)

if InsertDf.count()>0:
    pandasDFInsert = InsertDf.toPandas()
    batch=0
    for i in range(0,insert_numbr_splits ):
        insert_temp_df=pandasDFInsert.iloc[batch:batch+insert_each_len]
        df_inserts=insert_temp_df.to_json(orient='records')

        url = f"https://{profiseeHostUrl}/Profisee/rest/v1/Records/{ProfiseeEntity}"
        headers = {"Accept":"application/json -H","x-api-key":profiseeAPIPassword,"Content-Type": "application/json-patch+json"} 
 
        response = requests.post(url, headers=headers, data=df_inserts)
        print(response.status_code)
        
        httpCode=str(response.status_code)
        if response.status_code==201 or response.status_code==200 or response.status_code==207:
            print("successfully inserted")
            #logger.info("-Message :" + "Successfull_Insert_For_Batch: "+df_inserts)
        else:
            print("Not processed")
            raise ValueError("Failed to process the Batch Insert. Success response code not received from the API : " + httpCode)
            # To Do- Log Details-Timestamp|Batch Error|status-code-Logging Here
        
        i=i+1
        batch=batch+insert_each_len
        #sendLogs(logs_storage_path,filename)
else:
       print("No Records Found For Insertion")
       #sendLogs(logs_storage_path,filename)
