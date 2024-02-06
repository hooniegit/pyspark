
def create_dataframe(buffer:list, spark):
    from pyspark.sql import Row
    
    print(buffer) # test
    
    rdd = spark.sparkContext.parallelize([Row(**item) for item in buffer])
    df = spark.createDataFrame(rdd)

    df.show() # test
    
    return df
