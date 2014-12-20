package ydata

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import cern.colt.map.tdouble.OpenIntDoubleHashMap
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.{SparkContext, SparkEnv, sql}
import org.apache.spark.SparkContext._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3

class HashVector(initialCapacity: Int) extends OpenIntDoubleHashMap(initialCapacity) with Traversable[(Int, Double)] {
  override def isEmpty = super[OpenIntDoubleHashMap].isEmpty
  override def size = super[OpenIntDoubleHashMap].size

  def foreach[U](f: ((Int, Double)) => U) {
    val keys = this.keys()

    for (i <- 0 to keys.size() - 1) {
      val index = keys.getQuick(i).toInt

      f(index, this.get(index))
    }
  }
}

class DownpourSGD extends Actor {
  val log = Logging(context.system, this)
  var w = new Array[Float](DownpourSGD.D)// Vectors.zeros(DownpourSGD.D).toArray
  var n = new Array[Float](DownpourSGD.D)// Vectors.zeros(DownpourSGD.D).toArray

  def receive = {
    case "get_model" => sender ! (w, n)
    case delta: (HashVector, HashVector) => {
      val dw = delta._1
      val dn = delta._2

      dw.foreach(kv => w(kv._1) += kv._2.toFloat)
      dn.foreach(kv => n(kv._1) += kv._2.toFloat)
    }
    case _ => log.info("received undefined")
  }

}

object DownpourSGD {
  val D = math.pow(2, 20).toInt
  val Capacity = 1024

  def hashtrick(row: sql.Row): HashVector = {
    var vec: HashVector = new HashVector(Capacity)

    def checkNone(v: Any): String = {
      if (v == null)
        "NULL"
      else
        v.toString
    }

    row.zipWithIndex.map(col =>  {
      val nidx = (math.abs(MurmurHash3.stringHash(col._2.toString + "_"
      + checkNone(col._1))) % DownpourSGD.D)
      assert(nidx >= 0)
      vec.put(nidx, 1.0)
    })
    vec
  }

  def logloss(ypred: Double, ytrue: Double): Double = {
    val p = math.max(math.min(ypred, 1. - 10e-15), 10e-15)
    if (ytrue == 1.)
      -math.log(p)
    else
      -math.log(1. - p)
  }

  def predict(x: HashVector, w: Array[Float]): Double = {
    var wTx = 0.

    x.foreach { kv =>
      val idx = kv._1
      wTx += w(idx) * x.get(idx)
    }
    1. / (1. + math.exp(-math.max(math.min(wTx, 20.), -20.)))
  }

  def predict(path: String, X: SchemaRDD): SchemaRDD = {
      implicit val timeout = Timeout(5 seconds)
      val system = SparkEnv.get.actorSystem
      val sgd_u = system.actorSelection(path)
      val future = sgd_u.ask("get_model").mapTo[(Array[Float], Array[Float])]
      val result = Await.result(future, timeout.duration)
      var w = result._1
      val schema = sql.StructType(Seq(sql.StructField("ypred", sql.DoubleType, false)))
      val rowRDD = X.map(row => {
        val x = hashtrick(row)
        val p = predict(x, w)

        sql.Row(p)
      })
    X.sqlContext.applySchema(rowRDD, schema)
  }

  def fit(path: String, X: SchemaRDD, y: SchemaRDD, schema: sql.StructType = null): Double = {
    val alpha = 0.1

    def gradient(iterator: Iterator[(sql.Row, sql.Row)]): Iterator[Double] = {
      implicit val timeout = Timeout(5 seconds)
      val system = SparkEnv.get.actorSystem
      val sgd_u = system.actorSelection(path)
      val future = sgd_u.ask("get_model").mapTo[(Array[Float], Array[Float])]
      val result = Await.result(future, timeout.duration)
      var w = result._1
      var n = result._2
      var dw = new HashVector(Capacity)
      var dn = new HashVector(Capacity)
      var loss = 0.0

      while (iterator.hasNext) {
        val row = iterator.next()
        val x = hashtrick(row._1)
        val p = predict(x, w)
        val y = if (row._2.isNullAt(0)) 0 else row._2.getInt(0).toDouble

        // update local
        x.foreach(kv => {
          val idx = kv._1
          val up = math.abs(p - y)

          dn.put(idx, dn.get(idx) + up)
          n(idx) += up.toFloat

          val grad = - (p - y) * kv._2 * alpha / math.sqrt(n(idx))
          dw.put(idx, dw.get(idx) + grad)
          w(idx) += grad.toFloat
        })

        loss += logloss(p, y)
      }
      val msg = (dw, dn)
      sgd_u ! msg

      Seq(loss).toIterator
    }

    val count = X.count()
    val lloss = X.zip(y).mapPartitions(gradient).sum() / count.toDouble
    lloss
  }
}
