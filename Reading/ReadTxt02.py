# Reading data from text file using 
# spark.read.csv() function
# This function returns DataFrame

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('output.txt')

df.selectExpr("split(_c0,' ') as Text_Data_In_Rows_Using_CSV").show(4,False)