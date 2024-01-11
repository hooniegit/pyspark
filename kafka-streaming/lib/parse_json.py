
# create new dataframe
def receive_json(batchDF, batchId, spark):
    from pyspark.sql.functions import col
    from pyspark.sql.types import StringType
    import json
    
    try:
        # return value
        json_data = batchDF \
            .select(col("value").cast(StringType()).alias("value")) \
            .first()[0]
        
        # change datatype to JSON
        json_value = json.loads(json_data)
        # create RDD with JSON
        rdd_json = spark.sparkContext.parallelize([json_value])
        # create dataframe with RDD
        df_json = spark.read.json(rdd_json, multiLine=True)
        
        # test
        df_json.write \
            .format("console") \
            .save()               
        
    except Exception as E:
        print(f">>>>>>>> {E}")

# create schema & change datatype
def receive_json_schema(batchDF, batchId, spark):
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import col, from_json
    import json
    
    def infer_schema(json_data:dict):
        from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, ArrayType
        
        def infer_field(name, value):
            if isinstance(value, bool):
                return StructField(name, BooleanType(), True)
            elif isinstance(value, int):
                return StructField(name, IntegerType(), True)
            elif isinstance(value, list):
                element_type = infer_field(name, value[0]).dataType
                return StructField(name, ArrayType(element_type), True)
            elif isinstance(value, dict):
                nested_fields = [infer_field(sub_name, sub_value) for sub_name, sub_value in value.items()]
                return StructField(name, StructType(nested_fields), True)
            else:
                return StructField(name, StringType(), True)
        
        # create json schema
        fields = [infer_field(name, value) for name, value in json_data.items()]
        
        return StructType(fields)
    
    try:
        # create schema
        json_data = batchDF \
            .select(col('value').cast(StringType()).alias('value')) \
            .first()[0]
        schema = infer_schema(json.loads(json_data))
        
        # change datatype
        df = batchDF \
            .withColumn(col("key").cast(StringType()).alias("key")) \
            .withColumn("value", from_json("value", schema))
        
        # test
        df.write \
            .format("console") \
            .save()
        
    except Exception as E:
        print(f">>>>>>>> {E}")
