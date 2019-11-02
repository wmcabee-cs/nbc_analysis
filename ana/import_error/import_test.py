#!/usr/bin/env python
# coding: utf-8

import findspark
findspark.init()
import pyspark

import pprint
from pyspark.sql import SparkSession
import sys


# copy your _scporxy.so to
# /System/Library/Frameworks/Python.framework/Versions/2.6/lib/python2.6/lib-dynlo


def try_import(name):
    import sys, os, csv
    from pathlib import Path
    from toolz import concatv

    cwd = Path()

    reader = sorted(concatv(
        [('sys', 'script', sys.argv[0], name)],
        [('sys', 'executable', sys.executable, name)],
        [('path', 'cwd', str(cwd.absolute()), name)],
        sorted(('module', k, str(mod), name) for k, mod in sys.modules.items()),
        sorted(('environ', k, str(val), name) for k, val in dict(os.environ).items()),
    ))

    infile = cwd / f"{name}.csv"
    with infile.open('w') as fh:
        writer = csv.writer(fh)
        writer.writerow(['category', 'type', 'item', 'source'])
        for rec in reader:
            writer.writerow(rec)
    result = {'infile': str(infile),
              'record_cnt': len(reader)}
    print('>> starting import')
    import _scproxy
    return result


#ret = try_import('cmd_local')

spark = (SparkSession
         .builder
         .master("local")
         .appName("Import Test")
         .config('spark.driver.memory', '4g')
         .getOrCreate())

df = spark.createDataFrame([{'iteration': 1}])
rdd = df.rdd.map(lambda x: try_import('cmd_spark'))
ret = rdd.take(1)
pprint.pprint(ret)
