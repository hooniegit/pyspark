from pyspark.sql import SparkSession

# set spark master
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("spark://neivekim76.local:7077") \
    .getOrCreate()

# set kafka servers & topics
kafka_bootstrap_servers = 'localhost:9092'
input_kafka_topic = "spark_streaming"

# set buffer & batch
buffer, batch = [], []
buffer_size = 5

# read stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()
    
def set_buffer(batchDF, batchId, spark):
    # key	binary -> need to decode
    # value	binary -> need to decode
    # topic	string
    # partition	int
    # offset	long
    # timestamp	timestamp
    # timestampType	int
    # headers (optional)	array
    from lib.buffer.export_datas import export_all
    from lib.json.create_dataframe import create_dataframe
    from lib.dataframe.print_to_terminal import print_dataframe
 
    try:
        global buffer, batch
        data = export_all(batchDF=batchDF, spark=spark)
        buffer.append(data)
        batch.append(batchId)
        
        if len(buffer) >= buffer_size:
            df_buffer = create_dataframe(buffer=buffer, spark=spark)
            print_dataframe(df_buffer=df_buffer, spark=spark)
            print(">>>>>> Let's check out batches " + str(batch)) # need to fix
            buffer, batch = [], []
    
    except Exception as E:
        print(f">>>>>> Oops, error appeared.. \n{E}")

# set query
query = df \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: set_buffer(batchDF, batchId, spark)) \
    .start()

# await termination
query.awaitTermination()
