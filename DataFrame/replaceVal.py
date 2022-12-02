from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("Fill Null").getOrCreate()

my_list = [['male', 1, None], ['female', 2, 3],['male',3, 4]]
dp = pd.DataFrame(my_list, columns=['A','B','C'])
ds = spark.createDataFrame(my_list, ['A','B','C'])

print(dp.head())

ds.show()

dp.replace(['male','female'],[1,0],inplace=True)

print(dp.head())

ds.na.replace(['male','female'], ['1','0']).show()