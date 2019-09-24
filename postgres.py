from __future__ import print_function

import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: sort <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonSort")\
        .getOrCreate()

    # Function where input is import lines in Jupyter Notebook Code and output is libraraies

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    ls = lines.map(lambda x: x) \
    .filter(lambda x: 'import' in x) \
    .map(lambda x: x.split(' ')) \
    .map(lambda x: [x[i+1] for i in range(len(x)) if x[i]=='"import' or x[i]=='"from']) \
    .map(lambda x: x[0].split('.')).map(lambda x: x[0].split('\\')) \
    .map(lambda x: x[0]) \
    .map(lambda x: (x,1)) \
    .reduceByKey(lambda n,m: n+m).map(lambda x: x[0])

    output = ls.collect()

    OutF = open("OutFile.txt", "w")

    for l in output:
        OutF.write("%s" %l )
        OutF.write("\n")
    OutF.close()

    for l in output:
        print(l)

    spark.stop()
