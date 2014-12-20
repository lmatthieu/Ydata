#!/usr/bin/env python
#
__author__ = 'matthieu'

import os
import glob
import sys

def findPy4J(sparkpath):
    py4jglob = os.path.join(sparkpath, "python", "lib", "py4j-*-src.zip")
    try:
        return glob.iglob(py4jglob).next()
    except StopIteration:
        raise Exception("No py4j jar found at "+py4jglob+"; is SPARK_HOME set correctly?")

spark = os.getenv('SPARK_HOME')
if spark is None or spark == '':
    raise Exception('You must define SPARK_HOME with the location of the Spark installation')

sys.path.append(spark + "python/")
sys.path.append(findPy4J(spark))

import ydata

jar = os.path.join(os.path.dirname(os.path.realpath(ydata.__file__)), 'lib/Ydata_scala.jar')

if os.getenv('PYTHONPATH') is None:
    os.environ['PYTHONPATH'] = ""

os.environ['PYTHONPATH'] = os.environ['PYTHONPATH'] + ":" + spark + "/python:../" + ":" + findPy4J(spark)
os.environ['PYTHONSTARTUP'] = os.path.join(os.path.dirname(os.path.realpath(ydata.__file__)), 'shell.py')

if os.getenv('SPARK_SUBMIT_CLASSPATH') is None:
    os.environ['SPARK_SUBMIT_CLASSPATH'] = ""

os.environ['SPARK_SUBMIT_CLASSPATH'] = os.environ['SPARK_SUBMIT_CLASSPATH'] + ":" + jar
os.environ['IPYTHON'] = '1'

import argparse

parser = argparse.ArgumentParser(description='Booy Ydata context')
parser.add_argument('--notebook', dest='notebook', action='store_true', help='initialize Ydata context in IPython Notebook')
parser.add_argument('--core', dest='core', action='store', type=int, default=4, help='SparkContext nb cores')
parser.add_argument('--mem', dest='mem', action='store', type=str, default="1g", help='SparkContext executor memory')

args = parser.parse_args()

if args.notebook == False:
    os.system(os.path.join(spark, 'bin/pyspark --jars ' + jar))
else:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext
    from py4j.java_gateway import java_import

    conf = (SparkConf().setMaster("local[%d]" %args.core).setAppName("Yspark").set("spark.executor.memory", args.mem))
    sc = SparkContext(conf=conf)
    hsc = HiveContext(sc)
    java_import(hsc._jvm, "org.apache.spark.mllib.api.python.*")

