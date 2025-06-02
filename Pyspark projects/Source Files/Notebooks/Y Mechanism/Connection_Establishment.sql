-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## CONNECTION ESTABLISHMENT to S3

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/yuvan.shankar.m@accenture.com/Pyspark Projects/Secrets"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import boto3
-- MAGIC
-- MAGIC
-- MAGIC s3 = boto3.client(
-- MAGIC     's3',
-- MAGIC     aws_access_key_id=access_key,
-- MAGIC     aws_secret_access_key=secret_key
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     response = s3.list_objects_v2(Bucket="pysparkprojects", Prefix="OutputFiles/")
-- MAGIC     if 'Contents' in response:
-- MAGIC         print("✅ S3 connection successful. Found files:")
-- MAGIC         for obj in response['Contents']:
-- MAGIC             print(obj['Key'])
-- MAGIC             break
-- MAGIC     else:
-- MAGIC         print("✅ Connected but no files found in the path.")
-- MAGIC except Exception as e:
-- MAGIC     print("❌ Connection failed:", e)