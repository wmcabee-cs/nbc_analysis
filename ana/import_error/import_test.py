#!/usr/bin/env python
# coding: utf-8


def try_import(name):
    import sys, os, csv
    import json

    from pathlib import Path
    from toolz import concatv
    import _hashlib
    cwd = Path()
    reader = sorted(concatv(
        [('sys', 'script', sys.argv[0], name)],
        [('sys', 'executable', sys.executable, name)],
        [('sys', 'path', sys.path, name)],
        [('path', 'cwd', str(cwd.absolute()), name)],
        [('path', '_hashlib', str(_hashlib.__file__), name)],
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
    print(f'>> result={result}')
    import _scproxy
    # import numpy
    # import pandas

    return json.dumps(sys.path)


ret = try_import('cmd_local')
print(' local worked')
# exit('stopped after local')

################################################################################
# SPARK RUN
################################################################################
import findspark
findspark.init()
import pyspark

from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .master("local")
         .appName("Import Test")
         .config('spark.driver.memory', '4g')
         .getOrCreate())

df = spark.createDataFrame([{'iteration': 1}])
rdd = df.rdd.map(lambda x: try_import('cmd_spark'))
ret = rdd.take(1)
print(ret[0])
