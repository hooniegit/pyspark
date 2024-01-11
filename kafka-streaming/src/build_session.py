from pyspark.sql import SparkSession

# need to add packages
# org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 for spark 3.5.0

spark = SparkSession.builder \
    .appName("kafka_stream_demo") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("spark://neivekim76.local:7077") \
    .getOrCreate()