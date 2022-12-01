from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, HiveContext

# sc = SparkSession.builder.enableHiveSupport()
sc = SparkContext('local', 'example')
hc = HiveContext(sc)
tf1 = sc.textFile('hdfs://localhost:50000/data/MrBeast_youtube_stats.csv')
print(tf1.first())

hc.sql('use intg_cme_w')
spf = hc.sql('SELECT * FROM spf LIMIT 100')
print(spf.show(5))