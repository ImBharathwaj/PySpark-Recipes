import os
import sys
from operator import add

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/bharathwaj/Applications/spark-3.3.1-bin-hadoop2"

# Append pyspark  to Python Path
sys.path.append("/home/bharathwaj/Applications/spark-3.3.1-bin-hadoop2/python/pyspark")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)


sc = SparkContext('local', 'count app')
words = sc.parallelize(
    [
        'scala',
        'java',
        'hadoop',
        'spark',
        'akka',
        'spark vs hadoop',
        'pyspark',
        'pyspark and spark'
    ]
)

counts = words.count()
col1 = words.collect()
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print('1. Number of elements in RDD ->', counts)
print('2. Elements in RDD ->', col1)
print('3. Key value pair ->', mapping)
print('4. Filtered RDD ->', counts)
