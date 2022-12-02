# Creating RDD using createDataFrame() function
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Python Spark create RDD using createDataFrame()")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

Employee = spark.createDataFrame(
    [
        ("1", "Joe", "70000", "1"),
        ("2", "Henry", "80000", "2"),
        ("3", "Sam", "80000", "2"),
        ("4", "Max", "90000", "1")
    ],
    ["Id", "Name", "Salary", "DepartmentId"]
)
Employee.printSchema()
Employee.show()
