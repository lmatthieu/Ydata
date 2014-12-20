# Ydata

Ydata : Spark &amp; Python for data exploration

## Building

$ git clone https://github.com/lmatthieu/Ydata.git
$ cd Ydata/scala

Building the scala package:

$ sbt package

Sharing the JAR with Python:

$ cp scala/target/scala-2.10/ydata_2.10-0.4.jar python/ydata/lib/Ydata_scala.jar

## Testing

[IPython notebook example](http://nbviewer.ipython.org/github/lmatthieu/Ydata/blob/master/python/notebook/Ydata_demo.ipynb)
