package com.aamend.texata.bigquery

import org.apache.spark.sql.SparkSession
import com.samelamin.spark.bigquery._

/**
  * Created by antoine on 23/09/2017.
  */
object BigQueryDFHarness {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("texata").getOrCreate()
    val sc = spark.sparkContext
    @transient
    val conf = sc.hadoopConfiguration
    val bucket = conf.get("fs.gs.system.bucket")
    val sqlContext = spark.sqlContext

    // Load everything from a table
    val table = sqlContext.bigQueryTable("bigquery-public-data:samples.shakespeare")

    // Load results from a SQL query
    // Defaults to legacy SQL dialect
    // To use standard SQL, set  --conf spark.hadoop.USE_STANDARD_SQL_DIALECT=true
    val df = sqlContext.bigQuerySelect("SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare]")

    sqlContext.setBigQueryGcsBucket(bucket)
    df.saveAsBigQueryTable("pathogen-1320:wordcount_dataset.wordcount_output2")

  }
}
