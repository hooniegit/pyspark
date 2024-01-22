
def export_all(batchDF, spark):
    from pyspark.sql.functions import col
    from pyspark.sql.types import StringType

    first_row_dict = batchDF \
        .withColumn("key", col("key").cast(StringType())) \
        .withColumn("value", col("value").cast(StringType())) \
        .select(*batchDF.columns).first().asDict()
        
    print(first_row_dict) # test

    return first_row_dict

