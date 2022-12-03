from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName('Drop Column').getOrCreate()

dp = pd.read_csv("Advertising.csv")
ds = spark.read.csv(path="Advertising.csv", header=True, inferSchema=True)

drop_name = ['Newspaper', 'Sales']

print(dp.drop(drop_name, axis=1).head(4))
ds.drop(*drop_name).show(4)
