# Databricks notebook source
import requests
import json

# COMMAND ----------

dbutils.widgets.text("workspace_name", "")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("resource", "")
dbutils.widgets.text("scope", "")

# COMMAND ----------

workspace_name = dbutils.widgets.get("workspace_name")
pipeline_name = dbutils.widgets.get("pipeline_name")
resource = dbutils.widgets.get("resource")
scope = dbutils.widgets.get("scope")

# COMMAND ----------

SynapseDatabricksClientId=dbutils.secrets.get(scope=scope,key="SynapseDatabricksClientId")
SynapseDatabricksSecret=dbutils.secrets.get(scope=scope,key="SynapseDatabricksSecret")

# COMMAND ----------

# Get an OAuth2 token
def get_oauth_access_token(SynapseDatabricksClientId, SynapseDatabricksSecret,resource):
    AzureTenantId=dbutils.secrets.get(scope=scope,key="AzureTenantId")
    grant_type='client_credentials'
    url = f"https://login.microsoftonline.com/{AzureTenantId}/oauth2/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': grant_type,
        'client_id': SynapseDatabricksClientId,
        'client_secret': SynapseDatabricksSecret,
        'resource': resource
    }
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()['access_token']

# COMMAND ----------

# Trigger the Synapse pipeline
def trigger_synapse_pipeline(workspace_name, pipeline_name, access_token,params):
    url = f"https://{workspace_name}/pipelines/{pipeline_name}/createRun?api-version=2020-12-01"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'raw'
    }
    body=json.dumps(params) if params else '{}'
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()
