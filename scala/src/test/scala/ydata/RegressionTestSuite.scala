package ydata

import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FunSuite
import ydata.api.python.PythonYdataAPI

class RegressionTestSuite extends FunSuite with YdataSparkContext {

  test("readsql") {
    val hc = new HiveContext(sc)
    var table = hc.parquetFile("/tmp/data/avazu2/avtrain")

    table.registerTempTable("train")
    val X = hc.sql("select hour,C1,banner_pos,site_id,site_domain,site_category,app_id,app_domain,app_category,device_id,device_ip,device_model,device_type,device_conn_type,C14,C15,C16,C17,C18,C19,C20,C21 from train")
    var y = hc.sql("select click from train")
    var api = new PythonYdataAPI


    var table2 = hc.parquetFile("/tmp/data/avazu2/avtest")

    table2.registerTempTable("test")
    val X_test = hc.sql("select hour,C1,banner_pos,site_id,site_domain,site_category,app_id,app_domain,app_category,device_id,device_ip,device_model,device_type,device_conn_type,C14,C15,C16,C17,C18,C19,C20,C21 from test")
    val y_id = hc.sql("select id from test")

    val model = api.fitDSGD(X.toJavaSchemaRDD, y.toJavaSchemaRDD)
    api.predictDSGD(model, X_test.toJavaSchemaRDD)
  }
}
