# Databricks notebook source
import pyspark.sql.types as T
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# Event Hubs configuration
EH_NAMESPACE                    = ""
EH_NAME                         = ""

EH_CONN_SHARED_ACCESS_KEY_NAME  = "iothubowner"
EH_CONN_SHARED_ACCESS_KEY_VALUE = ""

EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
# Kafka Consumer configuration

EH_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
}

# PAYLOAD SCHEMA
schema = """messageId string,deviceId int, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"""




# COMMAND ----------

df = (
        spark.readStream.format("kafka")
        .options(**EH_OPTIONS)                                                         
        .load()                                                                         
        .withColumn('body', F.from_json(F.col('value').cast('string'), schema))
        .withColumn('timestamp', F.current_timestamp())
        .select(
            F.col("body.messageId").alias("messageID"),
            F.col("body.deviceId").alias("deviceId"),
            F.col("body.temperature").alias("temperature"),
            F.col("body.rpm").alias("rpm"),
            F.col("body.angle").alias("angle"),
            F.col("timestamp").alias("timestamp"),
        )
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/FileStore/test12345")
        .trigger(processingTime='0 seconds')
        .table("example.Bronze")
)

# COMMAND ----------


