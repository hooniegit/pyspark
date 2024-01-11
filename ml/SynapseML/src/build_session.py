import pyspark

# need to add packages
# com.microsoft.azure:synapseml_2.12:1.0.2

spark = pyspark.sql.SparkSession.builder.appName("SynapseML") \
            .master("spark://workspace:7077") \
            .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:1.0.2") \
            .getOrCreate()
