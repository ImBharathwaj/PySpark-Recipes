from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("Fill Null").getOrCreate()

my_list = [['male', 1, None], ['female', 2, 3],['male',3, 4]]
dp = pd.DataFrame(my_list, columns=['A','B','C'])
ds = spark.createDataFrame(my_list, ['A','B','C'])

# shows dataframe with old column names
ds.show(4)

# changing new column name to dataframe from pandas library
dp.columns = ['a','b','c']
print(dp.head(4))

# changing new column name to dataframe from pyspark library
ds.toDF('a','b','c').show(4)

# renaming one or more columns
mapping = {'A':'Newspaper','B':'Radio'}
dp.rename(columns=mapping).head(4)


new_names = [mapping.get(col, col) for col in ds.columns]
ds.toDF(*new_names).show(4)

ds.withColumnRenamed('A', 'Paper').show(4)