/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aamend.texata.bigquery

import com.google.cloud.hadoop.io.bigquery.output.{BigQueryOutputConfiguration, IndirectBigQueryOutputFormat}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryFileFormat, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.SparkSession

object BigQueryRDDHarness {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("texata").getOrCreate()
    val sc = spark.sparkContext

    @transient
    val conf = sc.hadoopConfiguration
    val projectId = conf.get("fs.gs.project.id")
    val bucketName = conf.get("fs.gs.system.bucket")
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucketName)

    conf.set("mapreduce.job.outputformat.class", classOf[IndirectBigQueryOutputFormat[_, _]].getName)

    // Truncate the table before writing output to allow multiple runs.
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, "WRITE_TRUNCATE")

    val fullyQualifiedInputTableId = "publicdata:samples.shakespeare"
    BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)

    val outputTableId = projectId + ":wordcount_dataset.wordcount_output"

    // Temp output bucket that is deleted upon completion of job.
    val outputGcsPath = "gs://" + bucketName + "/hadoop/tmp/bigquery/wordcountoutput"

    // Output configuration.
    // Let BigQueery auto-detect output schema (set to null below).
    BigQueryOutputConfiguration.configure(conf,
      outputTableId,
      null,
      outputGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      classOf[TextOutputFormat[_, _]])

    // Load data from BigQuery.
    val tableData = sc.newAPIHadoopRDD(
      conf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])

    // Perform word count.
    val wordCounts = tableData
      .map(entry => convertToTuple(entry._2))
      .reduceByKey(_ + _)

    // Display 10 results.
    wordCounts.take(10).foreach(l => println(l))

    // Write data back into a new BigQuery table.
    // IndirectBigQueryOutputFormat discards keys, so set key to null.
    wordCounts
      .map(pair => (null, convertToJson(pair)))
      .saveAsNewAPIHadoopDataset(conf)

  }

  // Helper to convert JsonObjects to (word, count) tuples.
  def convertToTuple(record: JsonObject): (String, Long) = {
    val word = record.get("word").getAsString.toLowerCase
    val count = record.get("word_count").getAsLong
    (word, count)
  }


  // Helper to convert (word, count) tuples to JsonObjects.
  def convertToJson(pair: (String, Long)): JsonObject = {
    val word = pair._1
    val count = pair._2
    val jsonObject = new JsonObject()
    jsonObject.addProperty("word", word)
    jsonObject.addProperty("word_count", count)
    jsonObject
  }


}
