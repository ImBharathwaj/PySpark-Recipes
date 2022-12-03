# Extracting and creating Dataframe from
# Pandas vs Spark

from pyspark.sql import SparkSession
import pandas as pd
import psycopg2 as ps

sql = """
    SELECT * FROM actor;
"""
conn = ps.connect(host="localhost", database="test", user="postgres", password="root")
cursor = conn.cursor()

dp = pd.read_sql(sql, conn)

spark = (
    SparkSession.builder.config(
        "spark.driver.extraClassPath", "/tmp/postgresql-42.2.6.jar"
    )
    .master("local")
    .appName("PySpark_Postgres_test")
    .getOrCreate()
)

df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/test")
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "actor")
    .option("user", "postgres")
    .option("password", "root")
    .load()
)

# Printing data extracted from pandas
print(dp.head(4))

# Printing data schema which is extracted from database
df.printSchema()

# Print data extracted from database using spark
df.show()

# Printing datatype of data from pandas
print(dp.dtypes)

# Printing datatype of data from spark
print(df.dtypes)
