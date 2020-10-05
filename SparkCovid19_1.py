from __future__ import print_function

import sys
from operator import add
import datetime
import time

from pyspark.sql import SparkSession 

# Set dates globally by default
start_date = time.strptime("2020-01-01", "%Y-%m-%d")
end_date = time.strptime("2020-04-08", "%Y-%m-%d")

# Filter invalid rows and rows not in specified date range
def custom_filter(v):
  if len(v)==4 and v[3].isnumeric():
		d = time.strptime(v[0], "%Y-%m-%d")
		if(d>=start_date and d<=end_date):
			return True

# Map (location, death_cases_count)
def custom_map(v):
  return (v[1],int(v[3]))

if __name__ == "__main__":

	# To find running time
  start_time = datetime.datetime.now()

  if len(sys.argv) != 5:
    print("Usage: Spark 1 <file>", file=sys.stderr)
    sys.exit(-1)
    
  spark = SparkSession\
        .builder\
        .appName("Death count by location in a given date range")\
        .getOrCreate()

  # Replace default date range by range given by user
  try:
    start_date = time.strptime(sys.argv[2], "%Y-%m-%d")
    end_date = time.strptime(sys.argv[3], "%Y-%m-%d")
  except ValueError:
    print("Invalid date given as input")
    sys.exit(1)
    
  # Map Reduce
  lines = spark.sparkContext.textFile(sys.argv[1])
  counts = lines.map(lambda x: x.split(',')) \
                .filter(lambda x: custom_filter(x)) \
                .map(lambda x: custom_map(x)) \
                .reduceByKey(add) \
                .sortByKey()
  
  # Collect output of RDD and print
  print("Output: ")
  output = counts.collect()
  for (word, count) in output:
      print("%s: %i" % (word, count))
  
  # Save file in output directory
  counts.saveAsTextFile(sys.argv[4])

  spark.stop()

  # To find total running time
  end_time = datetime.datetime.now()
  print("Time Taken: ", ((end_time-start_time).total_seconds()), "seconds \n")