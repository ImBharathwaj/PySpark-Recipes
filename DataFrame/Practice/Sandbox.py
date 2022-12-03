from __future__ import division

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import udf

sc = pyspark.SparkContext()
# sqlContext = pyspark.SQLContext(sc)

spark = SparkSession.builder.getOrCreate()

# Building a dataframe from Python list
DT1 = spark.createDataFrame(data=[(1, 2), (3, 4)], schema=("A", "B"))

# showing dataframe which is made
# Using show function
DT1.show()

sc.textFile("2015-12-12.csv", use_unicode=True).take(4)

dat = (
    sc.textFile("2015-12-12.csv", use_unicode=True)
    .map(lambda x: x.replace('"', ""))
    .map(lambda x: x.split(","))
)
print(dat.take(2))

DT2 = spark.createDataFrame(
    data=dat.filter(lambda x: x[0] != "date"),
    schema=dat.filter(lambda x: x[0] == "date").collect()[0],
)

# DT2.persist()
DT2.show(n=10)
print(DT2.columns)
print(DT2.dtypes)
DT3 = DT2.withColumn("size", DT2["size"].cast(IntegerType()))
DT4 = DT3.withColumn("date", DT3["date"].cast(DateType()))

print(DT3.dtypes)
DT3.show(5)

# Change column name
DT4 = DT2.withColumnRenamed("size", "size_new")
DT4.show(5)

# Order Ascending DataFrame by Specified Columns
DT3.sort(DT3.size.asc()).show(10)

# Order Descending DataFrame by Specified Columns
DT3.sort(DT3.size.desc()).show(10)

# Filtering and Aggregation
print(DT3.filter(DT3["size"] < 1000).count() / DT3.count())

print(DT3.filter(DT3["package"] == "ggplot2").count() / DT3.count())

print(DT3.groupBy("package").count().sort("count", ascending=False).show(10))

package_count = DT3.groupBy("package").count().sort("count", ascending=False)
package_count.show(10)

# Transform a Dataframe column using UDF
derive_perc = udf(lambda x: str(round(x * 100, 3)) + "%")

# or write a function with annotation
# @udf
# def derive_perc(x):
#   return(str(round(x * 100, 3))+'%')
package_count = package_count.withColumn(
    "perc", derive_perc(package_count["count"] / DT3.count())
)
package_count.show(10)

package_count.filter(package_count.package=='DT').show()

# Creates or replaces a local temporary view with a DataFrame.
# The lifetime of this temporary table is tied to the SparkSession that was used to create this DataFrame.
package_count.createOrReplaceTempView('package_count_sql_table')

# Basic Spark SQL Query - 1
query_result = sqlContext.sql(
    "SELECT perc FROM package_count_sql_table WHERE package = 'DF'")
print(query_result.collect())

# Basic Spark SQL Query - 2
query_result = sqlContext.sql(
    "SELECT * FROM package_count_sql_table WHERE count > 1000 ORDER BY count ASC")
print(query_result.show(5))

# Use the spark RDD way to process the results from Spark SQL query result
query_result.rdd.map(lambda x: x['package']+':'+x['perc']).take(10)
