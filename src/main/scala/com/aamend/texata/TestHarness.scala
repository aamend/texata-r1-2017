package com.aamend.texata

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TestHarness extends Harness with Logging {

  case class User(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("texata").getOrCreate()
    import spark.implicits._

    logInfo(s"Antoine, $greeting")
    val list = List(
      User("antoine", 34),
      User("marie", 35),
      User("charlotte", 8),
      User("apolline", 5)
    ).toDS()

    if(log.isDebugEnabled)
      list.collect().foreach(u => logDebug(u.toString))

    list.show()

  }

}
