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

package com.aamend.texata

import org.apache.spark.sql.SparkSession

object TexataEDA {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("texata-eda").getOrCreate()
    val sqlContext = spark.sqlContext

    val df = spark.read.option("header", "true").csv("/user/antoine/listenbrainz").cache
    df.createOrReplaceTempView("listenbrainz")
    sqlContext.cacheTable("listenbrainz")
    df.printSchema

    import java.sql.Timestamp
    import java.text.SimpleDateFormat
    import scala.util.Try

    val POP_CHECK: (String) => Int = (s: String) => {
      if(Try(s.trim.length).getOrElse(0) > 0) 1 else 0
    }

    val ASCIICLASS_HIGHGRAIN: (String) => String = (s: String) => {
      s.replaceAll("[a-z]", "a")
        .replaceAll("[A-Z]", "A")
        .replaceAll("[0-9]", "9")
        .replaceAll("\t", "T")
    }

    val ASCIICLASS_LOWGRAIN: (String) => String = (s: String) => {
      s.replaceAll("[a-z]+", "a")
        .replaceAll("[A-Z]+", "A")
        .replaceAll("[0-9]+", "9")
        .replaceAll("\t", "T")
    }

    val TO_TIME: (String) => Timestamp = (s: String) => {
      Try(new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s).getTime)).getOrElse(null)
    }

    val DAY_OF_MONTH: (Timestamp) => Int = (s: Timestamp) => {
      new org.joda.time.DateTime(s.getTime).getDayOfMonth
    }

    val MONTH_OF_YEAR: (Timestamp) => Int = (s: Timestamp) => {
      new org.joda.time.DateTime(s.getTime).getMonthOfYear
    }

    val YEAR: (Timestamp) => Int = (s: Timestamp) => {
      new org.joda.time.DateTime(s.getTime).getYear
    }

    val DAY_OF_WEEK: (Timestamp) => Int = (s: Timestamp) => {
      new org.joda.time.DateTime(s.getTime).getDayOfWeek
    }

    val HOUR_OF_DAY: (Timestamp) => Int = (s: Timestamp) => {
      new org.joda.time.DateTime(s.getTime).getHourOfDay
    }

    sqlContext.udf.register("POP_CHECK", POP_CHECK)
    sqlContext.udf.register("ASCIICLASS_HIGHGRAIN", ASCIICLASS_HIGHGRAIN)
    sqlContext.udf.register("ASCIICLASS_LOWGRAIN", ASCIICLASS_LOWGRAIN)
    sqlContext.udf.register("TO_TIME", TO_TIME)
    sqlContext.udf.register("DAY_OF_MONTH", DAY_OF_MONTH)
    sqlContext.udf.register("MONTH_OF_YEAR", MONTH_OF_YEAR)
    sqlContext.udf.register("YEAR", YEAR)
    sqlContext.udf.register("DAY_OF_WEEK", DAY_OF_WEEK)
    sqlContext.udf.register("HOUR_OF_DAY", HOUR_OF_DAY)

    import org.apache.spark.sql.functions._
    val popCheck = udf(POP_CHECK)
    df.filter(popCheck(col("tags")) === 1).select("tags").distinct.show
    // Records with non empty tags look a result of a badly formated CSV than a predefined taxonomy.
    // Tags / Genres cannot be used here

    val to_time = udf(TO_TIME)
    val year = udf(YEAR)

    val CLEAN_STR: (String) => String = (s: String) => {
      Try(s.toLowerCase().replaceAll("^\\W", "").trim).getOrElse("")
    }

    val clean = udf(CLEAN_STR)

    val cleanDF = df.filter(
      popCheck(col("tags")) === 0 &&
        popCheck(col("artist_mbids")) === 0 &&
        popCheck(col("release_mbid")) === 0 &&
        popCheck(col("user_name")) === 1 &&
        popCheck(col("track_name")) === 1 &&
        year(to_time(col("listened_at"))) >= 2015
    )
      .select("listened_at", "user_name", "artist_name", "track_name")
      .withColumn("user", clean(col("user_name"))).drop("user_name")
      .withColumn("artist", clean(col("artist_name"))).drop("artist_name")
      .withColumn("track", clean(col("track_name"))).drop("track_name")
      .withColumn("timestamp", to_time(col("listened_at"))).drop("listened_at")

    spark.read.parquet("/user/antoine/listenbrainz-clean")
    cleanDF.createOrReplaceTempView("brainz")

    cleanDF.groupBy("user").count.orderBy(desc("count")).show

    cleanDF.filter(col("user") !== "catcat").write.parquet("/user/antoine/listenbrainz-clean-catcat")
  }

}
