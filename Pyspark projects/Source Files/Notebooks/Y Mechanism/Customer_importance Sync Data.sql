-- Databricks notebook source
-- MAGIC %python
-- MAGIC %pip install --upgrade gdown

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count, row_number,lit
-- MAGIC from pyspark.sql.window import Window
-- MAGIC import os, gdown
-- MAGIC
-- MAGIC file_id = '1abe9EkM_uf2F2hjEkbhMBG9Mf2dFE4Wo'  
-- MAGIC output = '/dbfs/tmp/CustomerImportance.csv'  
-- MAGIC
-- MAGIC gdown.download(f'https://drive.google.com/uc?id={file_id}', output, quiet=False)
-- MAGIC
-- MAGIC customer_importance = spark.read.option("quote", "'").csv("/tmp/CustomerImportance.csv",header=True)
-- MAGIC
-- MAGIC transactions = spark.read.option("header", "true") \
-- MAGIC     .option("quote", "'").csv("dbfs:/FileStore/yuvan_may_2025/Projects/Pyspark Projects/CSV Files/Transactions/transactions.csv")
-- MAGIC
-- MAGIC Filtered_Data_customer = customer_importance.filter(customer_importance.fraud == 0)
-- MAGIC
-- MAGIC Filtered_Data_transactions = transactions.filter(transactions.fraud == 0)
-- MAGIC
-- MAGIC
-- MAGIC print("Total Transaction Count after removing the fraud transactions : " + str(Filtered_Data_transactions.count()))
-- MAGIC

-- COMMAND ----------

use yuvan_05_may

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ###  **PATTERN 3 LOGIC AND TABLE **OVERWRITE****

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, sum, when, trim, upper, lower
-- MAGIC
-- MAGIC #Filtered_Data_transactions.select("merchant").distinct().show(truncate=False)
-- MAGIC #print(Filtered_Data_transactions.select("merchant").distinct().count())
-- MAGIC
-- MAGIC grouped_df = Filtered_Data_transactions.groupBy("merchant").agg(
-- MAGIC     sum(when(col("gender")=="M", 1).otherwise(0)).alias("MALE"),
-- MAGIC     sum(when(col("gender")=="F", 1).otherwise(0)).alias("FEMALE")
-- MAGIC )
-- MAGIC
-- MAGIC grouped_df_filtered = grouped_df.filter((col("FEMALE") < col("MALE")))
-- MAGIC
-- MAGIC grouped_df_filtered.show(1)
-- MAGIC
-- MAGIC grouped_df_filtered.write.format("delta").mode("overwrite").saveAsTable("PATTERN3")
-- MAGIC
-- MAGIC print("Total Transaction count for Pattern 3 : " + str(grouped_df_filtered.count()))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## # ## PATTERN 2 LOGIC AND TABLE OVERWRITE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, sum, when, trim, upper, lower, count, avg
-- MAGIC
-- MAGIC Filtered_Data_customer.select("Target","Source").distinct().show(truncate=False)
-- MAGIC
-- MAGIC grouped_df = Filtered_Data_customer.groupBy("Target","Source").agg(
-- MAGIC     count(col("Target")).alias("TRANSACTION_COUNTS"),
-- MAGIC     avg(col("Weight")).alias("AVG_TRANSACTION_VALUE")
-- MAGIC )
-- MAGIC
-- MAGIC grouped_df_filtered = grouped_df.filter(
-- MAGIC     (col("TRANSACTION_COUNTS") >= 80) &
-- MAGIC     (col("AVG_TRANSACTION_VALUE") < 23)
-- MAGIC )
-- MAGIC
-- MAGIC grouped_df_filtered.show(1)
-- MAGIC
-- MAGIC grouped_df_filtered.write.format("delta").mode("overwrite").saveAsTable("PATTERN2")
-- MAGIC
-- MAGIC print("Total Transaction count for Pattern 2 : " + str(grouped_df_filtered.count()))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, sum, when, trim, upper, lower, count, avg , percentile_approx
-- MAGIC
-- MAGIC Merchant_transactions =Filtered_Data_customer.groupBy("Target").agg(
-- MAGIC     count(col("Target")).alias("TOTAL_TRANSACTION_COUNTS"))
-- MAGIC
-- MAGIC Merchant_50k = Merchant_transactions.filter(Merchant_transactions.TOTAL_TRANSACTION_COUNTS > 50000)
-- MAGIC
-- MAGIC filtered_merchant= Filtered_Data_customer.join(Merchant_50k, on="Target", how="inner")
-- MAGIC
-- MAGIC customer_txn_stats = filtered_merchant.groupBy("Source", "Target") \
-- MAGIC     .agg(
-- MAGIC         count("*").alias("customer_txn_count"),
-- MAGIC         avg("Weight").alias("avg_weight")
-- MAGIC     )
-- MAGIC
-- MAGIC percentiles = customer_txn_stats.groupBy("Target").agg(
-- MAGIC     percentile_approx("customer_txn_count", 0.99, 1000).alias("txn_99th"),
-- MAGIC     percentile_approx("avg_weight", 0.01, 1000).alias("weight_1st")
-- MAGIC )
-- MAGIC
-- MAGIC qualified_customers = customer_txn_stats.join(percentiles, on="Target") \
-- MAGIC     .filter(
-- MAGIC         (col("customer_txn_count") >= col("txn_99th")) &
-- MAGIC         (col("avg_weight") <= col("weight_1st"))
-- MAGIC     ) \
-- MAGIC     .select(
-- MAGIC         "Source",
-- MAGIC         "Target"
-- MAGIC     )
-- MAGIC
-- MAGIC qualified_customers.show(1)
-- MAGIC
-- MAGIC qualified_customers.write.format("delta").mode("overwrite").saveAsTable("PATTERN1")
-- MAGIC
-- MAGIC print("Total Transaction count for Pattern 1 : " + str(qualified_customers.count()))
-- MAGIC