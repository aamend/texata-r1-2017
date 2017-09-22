package com.aamend.texata

import org.apache.spark.internal.Logging
import org.scalatest.{FunSuite, Matchers}

class SparkTest extends FunSuite with Matchers with Logging {

  test("This is a test") {
    logInfo("INFO")
    logError("ERROR")
    logDebug("DEBUG")
    logWarning("WARN")
    1 should be(1)
  }

}
