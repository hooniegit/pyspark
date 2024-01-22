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

buffer, batch = [], []
buffer_size = 10

# 필요로 하는 데이터 json 형태로 처리, Buffer 내에 추가
# Buffer 추가 시점에 batchID 기록
# Buffer Size 도달 시 외부 함수 호출하여 작업 수행, 로그 또는 메세지 형태로 작업을 수행한 batchID 및 작업 시간 출력
def set_buffer(batchDF, batchId, spark):
    from lib.<library> import <module> 
    # 이 위치에서 dict 형태의 data 생성
    buffer.append(data)
    batch.append(batchId)
    
    if len(buffer) >= buffer_size:
        <module>(buffer, spark)
        print(batch)
        buffer, batch = [], []

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
