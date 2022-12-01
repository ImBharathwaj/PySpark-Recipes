# Creating RDD using .csv file
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Python Spark RDD with .csv file")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

df = spark.read.format("csv").load("/tmp/Dataset/MrBeast_youtube_stats.csv")

df.show(5)