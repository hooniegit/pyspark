from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# key	binary -> need to decode
# value	binary -> need to decode
# topic	string
# partition	int
# offset	long
# timestamp	timestamp
# timestampType	int
# headers (optional)	array

def demo_function(batchDF, batchId, spark):
    # decode & write(terminal)
    batchDF \
        .withColumn("key", col("key").cast(StringType())) \
        .withColumn("value", col("value").cast(StringType())) \
        .write \
        .format("console") \
        .save()

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("spark://neivekim76.local:7077") \
    .getOrCreate()

kafka_bootstrap_servers = 'localhost:9092'
input_kafka_topic = "demo"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()

query = df \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: demo_function(batchDF, batchId, spark)) \
    .start()

query.awaitTermination()