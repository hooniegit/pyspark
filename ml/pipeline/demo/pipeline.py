from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor

# create spark session
spark = SparkSession.builder.appName("Pipeline") \
    .master("spark://neivekim76.local:7077") \
    .getOrCreate()

# create dataset
data = [(1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0), (7.0, 8.0, 9.0), (7.0, 8.0, 9.0), (7.0, 8.0, 9.0), (7.0, 8.0, 9.0) ,(7.0, 8.0, 2.0), (7.0, 0.0, 9.0)
        , (3.0, 8.0, 9.0), (5.0, 8.0, 9.0), (7.0, 3.0, 9.0), (7.0, 0.0, 2.0), (7.0, 8.0, 6.0), (5.0, 8.0, 9.0), (7.0, 8.0, 9.0), (7.0, 8.0, 9.0), (0.0, 8.0, 9.0)
        , (3.0, 8.0, 9.0), (9.0, 8.0, 9.0), (7.0, 8.0, 1.0), (1.0, 8.0, 9.0), (7.0, 8.0, 7.0), (7.0, 8.0, 9.0)]
columns = ["feature1", "feature2", "label"]
df = spark.createDataFrame(data, columns)
train_data, test_data = df.randomSplit([0.8, 0.2], seed=123)

# define vector assembler
feature_cols = ["feature1", "feature2"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# define scaler
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

# define model
rf_regressor = RandomForestRegressor(featuresCol="scaled_features", labelCol="label")

# define pipeline
pipeline = Pipeline(stages=[vector_assembler, scaler, rf_regressor])

# train = create pipeline model
model = pipeline.fit(train_data)

# test
predictions = model.transform(test_data)
predictions.show()

# stop spark session
spark.stop()
