# Databricks notebook source
dbutils.widgets.text("BronzeTableName","")
dbutils.widgets.text("SourcePath","")
dbutils.widgets.text("TargetPath","")

bronze_table_name = dbutils.widgets.get("BronzeTableName")
source_path = dbutils.widgets.get("SourcePath")
target_path = dbutils.widgets.get("TargetPath")

# COMMAND ----------

df = spark.read.format("parquet").load(source_path)
df.write.format("delta").saveAsTable(bronze_table_name,path = target_path)

