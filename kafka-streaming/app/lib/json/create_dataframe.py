
def create_dataframe(data:list, spark):
    rdd_json = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd_json, multiLine=True)
    
    return df
