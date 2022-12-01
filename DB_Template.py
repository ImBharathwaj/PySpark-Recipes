from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = (
    SparkSession.builder.config("spark.jars", "/tmp/postgresql-42.2.6.jar")
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

df.printSchema()