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

package com.aamend.texata.profiler
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.aamend.texata.Harness
import com.google.common.hash.Hashing
import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.spark.sql.SQLContext

import scala.util.Try

object UDF extends Harness {

  val POP_CHECK: (String) => Int = (s: String) => {
    Try(s.trim.length).getOrElse(0)
  }

  /**
    *
    * Lookup based masks for single UTF bytes seen, to swap
    * raw codes for character class labels of my own choosing.
    * Like Japanese bytes = "J"
    *
    */
  val CLASS_FREQS: (String) => Array[String] = (s: String) => {
    s.map({
      case ch if Character.isIdeographic(ch) => "J"
      case ch if Character.isISOControl(ch) => "^"
      case ch if Character.isWhitespace(ch) => "T"
      case ch if Character.isDigit(ch) => "9"
      case ch if Character.isLetter(ch) => if (ch.isUpper) "A" else "a"
      case ch if Character.isDefined(ch) => "U"
      case everythingElse => everythingElse.toString
    }).toArray
  }

  /**
    * UPPER ascii characters => “A”,
    * LOWER ascii characters => “a”,
    * numbers => “9”,
    * all else left as is.
    */
  val ASCIICLASS_HIGHGRAIN: (String) => String = (s: String) => {
    s.replaceAll("[a-z]", "a")
      .replaceAll("[A-Z]", "A")
      .replaceAll("[0-9]", "9")
      .replaceAll("\t", "T")
  }

  /**
    * All UPPER ascii character blocks => “A”,
    * all LOWER ascii character blocks => “a”,
    * all number blocks => “9”,
    * all else left as is.
    */
  val ASCIICLASS_LOWGRAIN: (String) => String = (s: String) => {
    s.replaceAll("[a-z]+", "a")
      .replaceAll("[A-Z]+", "A")
      .replaceAll("[0-9]+", "9")
      .replaceAll("\t", "T")
  }

  /**
    * Hex representation (supports 16-bit char codes)
    * i.e. 0041, 0020, 6b45, etc
    *
    */
  val HEX: (String) => Array[String] = (s: String) => {
    s.map {
      case (i) if i > 65535 =>
        val hchar = (i - 0x10000) / 0x400 + 0xD800
        val lchar = (i - 0x10000) % 0x400 + 0xDC00
        f"$hchar%04x$lchar%04x"
      case (i) if i > 0 => f"$i%04x"
    }.toArray
  }

  /**
    * Unicode 16 multi-byte representation
    * i.e. \u0041, \u0020, \u6b45, etc
    *
    */
  val UNICODE: (String) => Array[String] = (s: String) => {
    s.map {
      case (i) if i > 65535 =>
        val hchar = (i - 0x10000) / 0x400 + 0xD800
        val lchar = (i - 0x10000) % 0x400 + 0xDC00
        f"\\u$hchar%04x\\u$lchar%04x"
      case (i) if i > 0 => f"\\u$i%04x"
    }.toArray
  }

  val TO_TIME: (String) => Timestamp = (s: String) => {
    Try(new Timestamp(new SimpleDateFormat(musicSdf).parse(s).getTime)).getOrElse(null)
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

  val VERTEX_ID: (String) => Long = (s: String) => {
    Hashing.sha256().hashString(s, StandardCharsets.UTF_8).asLong()
  }

  val CLEAN_STR: (String) => String = (s: String) => {
    Try(s.toLowerCase().replaceAll("^\\W", "").trim).getOrElse("")
  }

  val METAPHONE: (String) => String = (s: String) => {
    Try {
      val dm = new DoubleMetaphone()
      val parts = s.split("\\s")
      parts.map({ part =>
        val hash = dm.doubleMetaphone(part)
        if (hash == null) {
          ""
        } else {
          hash
        }
      }).sorted.mkString("#")
    }.getOrElse("#")
  }

  def build(sqlContext: SQLContext): Unit = {
    Map(
      "POP_CHECK" -> POP_CHECK,
      "CLASS_FREQS" -> CLASS_FREQS,
      "ASCIICLASS_HIGHGRAIN" -> ASCIICLASS_HIGHGRAIN,
      "ASCIICLASS_LOWGRAIN" -> ASCIICLASS_LOWGRAIN,
      "HEX" -> HEX,
      "UNICODE" -> UNICODE,
      "TO_TIME" -> TO_TIME,
      "DAY_OF_MONTH" -> DAY_OF_MONTH,
      "MONTH_OF_YEAR" -> MONTH_OF_YEAR,
      "YEAR" -> YEAR,
      "DAY_OF_WEEK" -> DAY_OF_WEEK,
      "HOUR_OF_DAY" -> HOUR_OF_DAY,
      "VERTEX_ID" -> VERTEX_ID,
      "CLEAN_STR" -> CLEAN_STR,
      "METAPHONE" -> METAPHONE
    ).foreach({ case (name, function) =>
      sqlContext.udf.register(name, function)
    })
  }

}
