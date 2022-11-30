# Word count using PySpark
import re
from operator import add

file_in = sc.textFile("/home/bharathwaj/Documents/Dataset/goodsdaily.csv")

# Count lines
print('number of lines in file: %s' % file_in.count())

# add up lengths of each line
chars = file_in.map(lambda s: len(s)).reduce(add)
print('number of characters in file: %s' % chars)

# Get words from the input file
words = file_in.flatMap(lambda line: re.split('\W+', line.lower().strip()))

# words of more than 3 characters
words = words.filter(lambda x: len(x) > 3)

# set count 1 per word
words = words.map(lambda x: (w,1))

# Reduce phase - sum count all the words
words = words.reduceByKey(add)

