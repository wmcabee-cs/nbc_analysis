# coding: utf-8
import findspark

findspark.init()


################################
# Spark function trying to run remotely with pandas
################################
def try_import(with_scproxy):
    # pandas import fails in spark if uncommented
    # import pandas

    # numpy import works in spark
    import numpy


    # hashlib import works in spark
    #  Note _hashlib.__file__ references same directory where scproxy.so is located
    import _hashlib
    so_paths = {'_hashlib.__file__': _hashlib.__file__}

    if with_scproxy:
        # this import fails when run in spark
        import _scproxy
        so_paths['_scproxy.__file__'] = _scproxy.__file__


    return so_paths


#################################
# Run function locally with scproxy import
#################################
print()
ret = try_import(with_scproxy=True)
print('>> _scproxy import worked outside of spark, so_paths:', ret)

#################################
# Run through spark with,  then without scproxy import
#################################
print()
from pyspark.sql import SparkSession, Row

spark = (SparkSession
         .builder
         .master("local")
         .appName("Import Test")
         # .config('spark.driver.memory', '4g')
         .getOrCreate())

df = spark.createDataFrame([Row(idx=1)])

print()
rdd = df.rdd.map(lambda r: try_import(with_scproxy=False))
ret = rdd.take(1)
print()
print('>> spark run worked without _scproxy. so_paths:', ret)

# run with _scproxy
print()
print('>> running spark with _scproxy import.')
rdd = df.rdd.map(lambda r: try_import(with_scproxy=True))
ret = rdd.take(1)
print('>> python crashes. never reaches this line. so_paths:', ret)
