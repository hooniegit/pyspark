
def create_dataframe(data:list, spark):
    import json
    
    json_list = json.loads(data)
    rdd_json = spark.sparkContext.parallelize(json_list)
    df = spark.read.json(rdd_json, multiLine=True)
    
    return df
