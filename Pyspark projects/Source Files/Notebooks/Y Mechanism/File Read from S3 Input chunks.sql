-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## FILE READ FROM S3 INPUT CHUNKS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC
-- MAGIC JOBstarttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/yuvan.shankar.m@accenture.com/Pyspark Projects/Secrets"

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/yuvan.shankar.m@accenture.com/Pyspark Projects/FINAL/Connection_Establishment"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC Processed_files = spark.table("yuvan_05_may.filename_processing")
-- MAGIC file_list = Processed_files.select("Filename").rdd.flatMap(lambda x: x).collect()
-- MAGIC
-- MAGIC file_set = set(file_list)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType
-- MAGIC input_filepath =""
-- MAGIC try:
-- MAGIC     response = s3.list_objects_v2(Bucket="pysparkprojects", Prefix="OutputFiles/Input_chunks")
-- MAGIC     if 'Contents' in response:
-- MAGIC         print("✅ S3 connection successful. Found files:")
-- MAGIC         for obj in response['Contents']:
-- MAGIC             splits = obj['Key'].split('/')
-- MAGIC             filename_csv = splits[-1]
-- MAGIC             if filename_csv.endswith(".csv") and filename_csv not in file_set :
-- MAGIC                 print(obj['Key'])
-- MAGIC                 fullpath = f"s3://pysparkprojects/"
-- MAGIC                 insertnew_record = spark.createDataFrame([(filename_csv, 'P')],schema=StructType([
-- MAGIC                 StructField("Filename", StringType(), True),
-- MAGIC                 StructField("Process_status", StringType(), True)
-- MAGIC                 ]))
-- MAGIC                 input_filepath = fullpath+ obj['Key']
-- MAGIC                 insertnew_record.write.mode("append").saveAsTable("yuvan_05_may.filename_processing")
-- MAGIC                 break
-- MAGIC             
-- MAGIC     else:
-- MAGIC         print("✅ Connected but no files found in the path.")
-- MAGIC except Exception as e:
-- MAGIC     print("❌ Connection failed:", e)
-- MAGIC
-- MAGIC if input_filepath == "" :
-- MAGIC     dbutils.notebook.exit("Stopped early due to No Records Found.")
-- MAGIC
-- MAGIC
-- MAGIC dbutils.jobs.taskValues.set(key="filename_csv", value=filename_csv)
-- MAGIC dbutils.jobs.taskValues.set(key="input_filepath", value=input_filepath)
-- MAGIC dbutils.jobs.taskValues.set(key="JOBstarttime", value=JOBstarttime)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema = StructType([
-- MAGIC     StructField("YStartTimeIST", StringType(), True),     # or TimestampType() if timestamps
-- MAGIC     StructField("detectionTimeIST", StringType(), True),  # or TimestampType()
-- MAGIC     StructField("patternId", StringType(), True),
-- MAGIC     StructField("ActionType", StringType(), True),
-- MAGIC     StructField("CustomerName", StringType(), True),
-- MAGIC     StructField("MerchantId", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC empty_df = spark.createDataFrame([], schema)
-- MAGIC
-- MAGIC namesplit = filename_csv.split('.')
-- MAGIC
-- MAGIC filename_csv = namesplit[0]
-- MAGIC
-- MAGIC table_name = f"yuvan_05_may.{filename_csv}"
-- MAGIC
-- MAGIC empty_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC ALTER TABLE {table_name} SET TBLPROPERTIES (
-- MAGIC   'delta.columnMapping.mode' = 'name',
-- MAGIC   'delta.minReaderVersion' = '2',
-- MAGIC   'delta.minWriterVersion' = '5'
-- MAGIC )
-- MAGIC """)
-- MAGIC
-- MAGIC print(f"Table Created. TABLE {filename_csv}")