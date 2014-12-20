package ydata.api.python

import akka.actor._
import org.apache.spark.SparkEnv
import org.apache.spark.sql.api.java.JavaSchemaRDD
import ydata.DownpourSGD

class PythonYdataAPI extends Serializable {

  def fitDSGD(X: JavaSchemaRDD, y: JavaSchemaRDD): ActorRef = {
    val system = SparkEnv.get.actorSystem

    val sgd = system.actorOf(Props[DownpourSGD])

    val loss = DownpourSGD.fit(sgd.path.toString, X.schemaRDD, y.schemaRDD)

    println("************** LogLoss " + loss)
    sgd
  }

  def predictDSGD(model: ActorRef, X: JavaSchemaRDD): JavaSchemaRDD = {
     DownpourSGD.predict(model.path.toString, X.schemaRDD).toJavaSchemaRDD
  }
}
