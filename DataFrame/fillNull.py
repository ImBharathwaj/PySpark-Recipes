from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("Fill Null").getOrCreate()

my_list = [['male', 1, None], ['female', 2, 3],['male',3, 4]]
dp = pd.DataFrame(my_list, columns=['A','B','C'])
ds = spark.createDataFrame(my_list, ['A','B','C'])

print(dp.head())
print()
print('-'*80)
print()
ds.show()

# Use fillna function to fill the the 
# null fields by passing optional argument
# This fillna function works with both 
# pandas and pyspark framework

ds.fillna(-99).show()