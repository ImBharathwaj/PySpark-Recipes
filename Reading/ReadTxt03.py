# Reading data from text file using 
# spark.read.format() function
# This function returns DataFrame
# Syntax : spark.read.format('text').load(path=None, format=None, schema=None, **options)
# Parameters:
#   path: string of input file path and name
#   format: this is optional for format of the file. default is parquet
#   Schema: this is optional pyspark.sql.types.StructType for input schema
#   options: all other string options

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.format('text').load('output.txt')

df.selectExpr("split(value,' ') as Text_Data_In_Rows_Using_format_load").show(4,False)