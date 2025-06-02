-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/yuvan.shankar.m@accenture.com/Pyspark Projects/FINAL/Connection_Establishment"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Chunk_Limit = int(dbutils.jobs.taskValues.get(taskKey="path_configuration", key="Chunks_Size", debugValue=0))
-- MAGIC Des_Chunk_path = str(dbutils.jobs.taskValues.get(taskKey="path_configuration", key="Destination", debugValue=""))
-- MAGIC temp_Chunk_path = str(dbutils.jobs.taskValues.get(taskKey="path_configuration", key="Temporary", debugValue=""))
-- MAGIC bucket_name = str(dbutils.jobs.taskValues.get(taskKey="path_configuration", key="Bucket_Name", debugValue=""))
-- MAGIC
-- MAGIC csv_filepath = str(dbutils.jobs.taskValues.get(taskKey="Input_Read", key="input_filepath", debugValue=""))
-- MAGIC filename_csv = str(dbutils.jobs.taskValues.get(taskKey="Input_Read", key="filename_csv", debugValue=""))
-- MAGIC JOBstarttime = str(dbutils.jobs.taskValues.get(taskKey="Input_Read", key="JOBstarttime", debugValue=""))
-- MAGIC
-- MAGIC
-- MAGIC print("Des_Chunk_path:", Des_Chunk_path)
-- MAGIC print("temp_Chunk_path:", temp_Chunk_path)
-- MAGIC print("bucket_name:", bucket_name)
-- MAGIC print("csv_filepath:", csv_filepath)
-- MAGIC print("filename_csv:", filename_csv)
-- MAGIC print("JOBstarttime:", JOBstarttime)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.functions import count, row_number,lit
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC namesplit = filename_csv.split('.')
-- MAGIC
-- MAGIC filename_csv = namesplit[0]
-- MAGIC
-- MAGIC window_spec = Window.orderBy(lit(1)) 
-- MAGIC
-- MAGIC Dataframe_table = spark.sql(f"SELECT * FROM yuvan_05_may.{filename_csv}")
-- MAGIC
-- MAGIC Dataframe_table = Dataframe_table.select(
-- MAGIC     F.col("YStartTimeIST").alias("YStartTime(IST)"),
-- MAGIC     F.col("detectionTimeIST").alias("detectionTime(IST)"),
-- MAGIC     "patternId",
-- MAGIC     "ActionType",
-- MAGIC     "CustomerName",
-- MAGIC     "MerchantId"
-- MAGIC )
-- MAGIC
-- MAGIC processed_dataframe = Dataframe_table.withColumn("unique_id", row_number().over(window_spec))
-- MAGIC
-- MAGIC print("Total Detection Count : " + str(processed_dataframe.count()))
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/yuvan.shankar.m@accenture.com/Pyspark Projects/FINAL/FolderCreationforTempandDestination"

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/yuvan.shankar.m@accenture.com/Pyspark Projects/FINAL/File Writes to S3"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"DROP TABLE yuvan_05_may.{filename_csv}")
-- MAGIC
-- MAGIC print(f'TABLE DROPPED {filename_csv}')