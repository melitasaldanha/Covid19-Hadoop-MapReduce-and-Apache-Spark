from __future__ import print_function

import sys
from operator import add
import datetime
import csv

from pyspark.sql import SparkSession 

# Map (location, registered_cases_count)
def custom_map1(v):
  return (v[1], int(v[2]))

# Map (location, registered_cases_per_1_million)
def custom_map2(x):
  k, v = x
  hm = broadcastVar.value

  if k in hm.keys():
    pop = hm[k]
    if(pop>0):
      new_v = ((v*1000000)/pop)
      return (k, new_v)


if __name__ == "__main__":

	# To find running time
  start_time = datetime.datetime.now()

  if len(sys.argv) != 4:
      print("Usage: Spark 2 <file>", file=sys.stderr)
      sys.exit(-1)
  
  spark = SparkSession\
      .builder\
      .appName("Registered cases per 1 million population")\
      .getOrCreate()

  # Make hashmap for population of each location
  d = {}
  pop_file = csv.reader(open(sys.argv[2], 'r'))
  for row in pop_file:
    if(row[4].isnumeric()):
      d[row[1]] = int(row[4])

  # Broadcast the hashmap
  broadcastVar = spark.sparkContext.broadcast(d)

  # Map Reduce
  lines = spark.sparkContext.textFile(sys.argv[1])
  counts = lines.map(lambda x: x.split(',')) \
                .filter(lambda x: (len(x)==4 and x[2].isnumeric())) \
                .map(lambda x: custom_map1(x)) \
                .reduceByKey(add) \
                .map(lambda x: custom_map2(x)) \
                .filter(lambda x: x!=None) \
                .sortByKey()
  
  # Collect output of RDD and print
  print("Output: ")
  output = counts.collect()
  for (word, count) in output:
      print("%s: %f" % (word, count))
  
  # Save file in output directory
  counts.saveAsTextFile(sys.argv[3])

  spark.stop()

  # To find total running time
  end_time = datetime.datetime.now()
  print("Time Taken: ", ((end_time-start_time).total_seconds()), "seconds \n")