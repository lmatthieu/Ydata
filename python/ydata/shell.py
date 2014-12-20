__author__ = 'matthieu'

from pyspark.sql import HiveContext, SQLContext
from py4j.java_gateway import java_import

hsc = HiveContext(sc)

java_import(hsc._jvm, "org.apache.spark.mllib.api.python.*")

print('\n')

print("""
       __  __    __      __
       \ \/ /___/ /___ _/ /_____ _
        \  / __  / __ `/ __/ __ `/
        / / /_/ / /_/ / /_/ /_/ /
       /_/\__,_/\__,_/\__/\__,_/
""")
print('Running Ydata version 0.4')
print('A Ydata context is available as hsc')
