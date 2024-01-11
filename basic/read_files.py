from pyspark.sql import SparkSession

# build spark session
spark = SparkSession.builder \
    .appName("ReadFiles") \
    .master("spark://neivekim76.local:7077") \
    .getOrCreate()

# define path
# local_dir = file:// 
# hdfs_dir = hdfs://
# s3a_dir = s3a://bucket/

# read json
json_path = "file://"
df = spark.read.option("multiline", "true").json(json_path)

# read csv
csv_path = "file://"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# read parquet
parquet_path = "file://"
df = spark.read.parquet(parquet_path)
