from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("Fill Null").getOrCreate()
dp = pd.read_csv("Advertising.csv")
ds = spark.read.csv(path="Advertising.csv", header=True, inferSchema=True)
print(dp[dp.Newspaper < 20].head(4))
print(ds[ds.Newspaper < 20].show(4))

print(dp[(dp.Newspaper<20) & (ds.TV > 100)].head(4))
print(dp[(dp.Newspaper<20) & (ds.TV > 100)].head(4))