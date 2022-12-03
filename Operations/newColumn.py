from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("New Column").getOrCreate()

dp = pd.read_csv("Advertising.csv")
ds = spark.read.csv(path="Advertising.csv", header=True, inferSchema=True)

dp["tv_norm"] = dp.TV / sum(dp.TV)
print(dp.head(4))

ds.withColumn("tv_norm", ds.TV / ds.groupBy().agg(F.sum("TV")).collect()[0][0])
ds.show(5)

dp["cond"] = dp.apply(
    lambda c: 1 if ((c.TV > 100) & (c.Radio < 40)) else 2 if c.Sales > 10 else 3, axis=2
)

ds.withColumn(
    "cond",
    F.when((ds.TV > 100) & (ds.Radio < 40), 1).when(ds.Sales > 10, 2).otherwise(3),
)