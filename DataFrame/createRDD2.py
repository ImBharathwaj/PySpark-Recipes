# Creating RDD using parallelize() function
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Python Spark create RDD example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

myData = spark.sparkContext.parallelize([(1, 2), (3, 4), (5, 6), (7, 8), 9, 10])
print(myData.collect())
