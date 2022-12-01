# Reading data from text file using 
# spark.read.text() function
# This function returns DataFrame

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df = spark.read.text('output.txt')

df.selectExpr("split(value,' ') as Text_Data_In_Rows_Using_Text").show(4,False)
