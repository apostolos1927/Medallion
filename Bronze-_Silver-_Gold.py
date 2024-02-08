# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Deduplication
# MAGIC
# MAGIC Structured Streaming provides exactly-once processing guarantees. However many source systems will send duplicate records
# MAGIC
# MAGIC -We need to identify and drop the duplicates in streaming data
# MAGIC
# MAGIC -Use watermarking to manage state information
# MAGIC
# MAGIC -Write an insert-only merge to prevent inserting duplicate records into a Delta table using a `foreachBatch` to perform a streaming upsert

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Duplicate Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM example.Bronze

# COMMAND ----------

total = (spark.read
              .table("example.Bronze")
              .groupBy(["deviceId"])
              .count())

total.display()

# COMMAND ----------

new_total = (spark.read
                  .table("example.Bronze")
                  .filter("deviceId = 2")
                  .dropDuplicates(["deviceId"])
).display()

# COMMAND ----------

after_dedup = (spark.read
                  .table("example.Bronze")
                  .dropDuplicates(["deviceId"])
                  )

after_dedup.display()

# COMMAND ----------

# MAGIC %md
# MAGIC We are choosing to apply deduplication at the silver rather than the bronze level. The reason is our bronze table retains a history of the true state of our streaming source. This allows us to recreate any state of our downstream system, if necessary, and prevents potential data loss due to overly strict quality enforcement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Streaming Read on the Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC When dealing with streaming deduplication we need to ensure that in each micro-batch:
# MAGIC - No duplicate records exist in the microbatch
# MAGIC - Records to be inserted are not already in the target table
# MAGIC
# MAGIC Spark Structured Streaming can track state information to ensure that duplicate records do not exist within or between microbatches.  Applying a watermark of appropriate duration allows us to only track state information for a window of time in which we reasonably expect records could be delayed. 

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("example.Bronze")
                   .withWatermark("timestamp", "10 seconds")
                   .dropDuplicates(["deviceId"]))
deduped_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Only Merge
# MAGIC Delta Lake has optimized functionality for insert-only merges. This operation is ideal for de-duplication: define logic to match on unique keys, and only insert those records for keys that don't already exist.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE example.Bronze

# COMMAND ----------

# %sql
# DROP TABLE spark_catalog.example.Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE spark_catalog.example.Silver (
# MAGIC   messageID STRING,
# MAGIC   deviceId INT,
# MAGIC   temperature DOUBLE,
# MAGIC   rpm DOUBLE,
# MAGIC   angle DOUBLE,
# MAGIC   timestamp TIMESTAMP)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Microbatch Function for foreachBatch
# MAGIC
# MAGIC The Spark Structured Streaming foreachBatch method allows users to define custom logic when writing.
# MAGIC
# MAGIC The logic applied during foreachBatch addresses the present microbatch as if it were a batch (rather than streaming) data.
# MAGIC

# COMMAND ----------

f = (spark.readStream
                   .option("skipChangeCommits", "true")
                   .table("example.Bronze")
                   .withWatermark("timestamp", "10 seconds")
                   .dropDuplicates(["deviceId"])
                   .writeStream
                   .foreachBatch(upsert_query)
                   .outputMode("update")
                   .option("checkpointLocation", f"dbfs:/FileStore/test")
                   .trigger(once=True)
                   .start())

f.awaitTermination()

# COMMAND ----------

def upsert_query(microBatchDF, batchID):
    microBatchDF.createTempView("bronze_layer")
    
    query = """
         MERGE INTO example.Silver a
         USING bronze_layer b
         ON a.deviceId=b.deviceId
         WHEN NOT MATCHED THEN INSERT *
    """
    # access the local spark session in foreachbatch
    microBatchDF._jdf.sparkSession().sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM example.Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE example.Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO example.Bronze VALUES (10,10,10,9,9,'2024-02-01T11:27:23.184+00:00')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE example.Bronze 
# MAGIC SET temperature=54 WHERE deviceId=10

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Enforcement
# MAGIC
# MAGIC Additional quality checks can be helpful to ensure that only data that meets your expectations makes it into your Lakehouse.
# MAGIC - Add check constraints to Delta tables
# MAGIC - Describe and implement a quarantine table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Constraints
# MAGIC
# MAGIC Databricks allows table constraints to be set on Delta tables.

# COMMAND ----------

# MAGIC  %sql
# MAGIC  use example;
# MAGIC  SHOW TABLES

# COMMAND ----------

# MAGIC  %md
# MAGIC  If these exist, table constraints will be listed in the extended table description.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED example.Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE example.Bronze ADD CONSTRAINT rpm_within_range CHECK (rpm>0);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED example.Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC But what happens if the conditions of the constraint aren't met?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake will prevent us from applying a constraint that existing records violate.

# COMMAND ----------

import pyspark
try:
    spark.sql("ALTER TABLE example.Bronze ADD CONSTRAINT validangle CHECK (angle > 100);")
    raise Exception("Expected failure")

except pyspark.sql.utils.AnalysisException as e:
    print("Failed as expected...")
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED example.Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC How do we deal with this? 
# MAGIC
# MAGIC We could manually delete offending records and then set the check constraint, or set the check constraint before processing data from our bronze table.
# MAGIC
# MAGIC However, if we set a check constraint and a batch of data contains records that violate it, the job will fail and we'll throw an error.
# MAGIC
# MAGIC If our goal is to identify bad records but keep streaming jobs running, we'll need a different solution.
# MAGIC
# MAGIC One idea would be to quarantine invalid records.
# MAGIC
# MAGIC Note that if you need to remove a constraint from a table, the following code would be executed.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE example.Bronze DROP CONSTRAINT rpm_within_range;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantining
# MAGIC
# MAGIC The idea of quarantining is that bad records will be written to a separate location.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM example.Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use example;
# MAGIC CREATE TABLE IF NOT EXISTS rpm_quarantine(
# MAGIC     messageID STRING,
# MAGIC   deviceId INT,
# MAGIC   temperature DOUBLE,
# MAGIC   rpm DOUBLE,
# MAGIC   angle DOUBLE,
# MAGIC   timestamp TIMESTAMP)

# COMMAND ----------

df_bronze = spark.sql("SELECT * FROM example.Bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE example.Silver;
# MAGIC TRUNCATE TABLE example.rpm_quarantine;

# COMMAND ----------

df_bronze.filter(df_bronze.rpm <= 50).write.format("delta").mode("append").saveAsTable("example.Silver")
df_bronze.filter(df_bronze.rpm > 50).write.format("delta").mode("append").saveAsTable("rpm_quarantine")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from example.rpm_quarantine

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from example.Silver

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Flagging
# MAGIC To avoid multiple writes and managing multiple tables, you may choose to implement a flagging system to warn about violations while avoiding job failures.
# MAGIC
# MAGIC Flagging is a low touch solution with little overhead.

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we'll just insert this logic as an additional transformation on a batch read of our bronze data to preview the output.

# COMMAND ----------

from pyspark.sql import functions as F
deduped_df = (spark.read
                  .table("example.Bronze")
                  .filter("deviceId >=2")
                  .select("*", F.when(F.col("rpm") >= 50, "RPM greater than 50")
                                  .otherwise("Less than 50")
                                  .alias("rpm_check"))
                  .dropDuplicates(["deviceId"]))

display(deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Stored Views
# MAGIC
# MAGIC Stored views differ from DataFrames and temp views by persisting to a database.Views register the logic required to calculate a result (which will be evaluated when the query is executed). The query always uses the latest version of each data source.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW BronzeView 
# MAGIC  (temperature COMMENT 'temperature comment', rpm COMMENT 'rpm comment')
# MAGIC     COMMENT 'View Comment'
# MAGIC     AS SELECT temperature, rpm
# MAGIC     FROM example.Bronze
# MAGIC     WHERE deviceId >2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from BronzeView

# COMMAND ----------

spark.table("BronzeView").explain("formatted")

# COMMAND ----------

DESCRIBE EXTENDED BronzeView

# COMMAND ----------

# MAGIC %md
# MAGIC When we execute a query against this view, we will process the plan to generate the logically correct result.
# MAGIC
# MAGIC Note that while the data may end up in the Delta Cache, this result is not guaranteed to be persisted, and is only cached for the currently active cluster.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM example.Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC # Materialized Gold Tables
# MAGIC
# MAGIC  Rather than caching the results to the view for quick access, results are stored in Delta Lake for efficient deserialization.
# MAGIC
# MAGIC Gold tables refer to highly refined, generally aggregate views of the data persisted to Delta Lake.
# MAGIC
# MAGIC These tables are intended to drive core business logic, dashboards, and applications.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE MATERIALIZED VIEW MAT_VIEW
# MAGIC AS
# MAGIC SELECT deviceId, MIN(temperature) min_temperature, MEAN(rpm) avg_rpm, MAX(angle) max_angle, COUNT(*) count_records
# MAGIC FROM example.Bronze
# MAGIC GROUP BY deviceId

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MAT_VIEW

# COMMAND ----------

(spark.readStream
      .table("example.Bronze")
      .createOrReplaceTempView("TEMP_BRONZE"))

# COMMAND ----------

df_final = spark.sql("""
    SELECT deviceId, MIN(temperature) min_temperature, MEAN(rpm) avg_rpm, MAX(angle) max_angle, COUNT(*) count_records
    FROM TEMP_BRONZE
    GROUP BY deviceId
    """)

(df_final
     .writeStream
     .format("delta")
     .option("checkpointLocation", "dbfs:/FileStore/mviewtest")
     .outputMode("complete")
     .trigger(availableNow=True)
     .table("final_table")
     .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC Using trigger-available-now logic with Delta Lake, we can ensure that we'll only calculate new results if records have changed in the upstream source tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM final_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE final_table
