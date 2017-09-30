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

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.util.Try

object TexataCheesyDJ {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("texata-eda").getOrCreate()
    import org.apache.spark.mllib.rdd.RDDFunctions._

    val df = spark.read.parquet("/user/antoine/listenbrainz-clean-catcat")
    val transitions = df.map(r => {
      (r.getAs[String]("user"), r.getAs[String]("artist") + " - " + r.getAs[String]("track"), r.getAs[java.sql.Timestamp]("timestamp").getTime)
    }).rdd.sortBy(s => (s._1, s._3)).sliding(2).filter(a => {
      val a1 = a.head
      val a2 = a.last
      a1._1 == a2._1 && a2._3 < a1._3 + 15 * 60 * 1000L
    }).map({ a =>
      ((a.head._2, a.last._2), 1)
    }).reduceByKey(_+_).filter(a => a._1._1 != a._1._2).cache()

    transitions.sortBy(_._2, ascending = false).take(20).foreach(println)

    // Given the number of vertices, generating unique Ids and doing successive joins will not be possible in the remaining 20mn.
     //Instead we generate IDs and limit the number of collision using SHA256 - If collision occurs, we can phone NSA as surely too same SHA would be interested for them :)

    val VERTEX_ID: (String) => Long = (s: String) => {
      Hashing.sha256().hashString(s, StandardCharsets.UTF_8).asLong()
    }

    val vertices = transitions.keys.keys.union(transitions.keys.values).distinct.map(s => (VERTEX_ID(s), s))
    val edges = transitions.map({case ((from, to), count) =>
      Edge(VERTEX_ID(from), VERTEX_ID(to), count)
    })

    val graph = Graph.apply(vertices, edges).cache()
    println(s"graph: V[${graph.vertices.count()}] E[${graph.edges.count()}]")

    graph.connectedComponents().vertices.values.distinct().count()

    vertices.filter(_._2.toLowerCase().contains("metal")).take(20).foreach(println) // 1154612932651817562 //eagles of death metal - 9. the deuce
    vertices.filter(_._2.toLowerCase().contains("mozart")).take(20).foreach(println) // -4621630949908535470 //wolfgang amadeus mozart - ii. kyrie eleison

    val seed = VERTEX_ID("-4621630949908535470")
    ShortestPaths.run(graph, Seq(seed)).vertices.map(a => (a._1, a._2.get(seed))).filter(_._2.isDefined).mapValues(_.get).createOrReplaceTempView("mozart")

    vertices.filter({ case (vId, vData) =>
      Set(
        1154612932651817562L,
      -362071403574830498L,
      -250480516945143425L,
      8170382784866980737L,
      1378812299461117959L,
      -4952029787374858646L,
      5527647851524983860L,
      8584955346000525442L
      ).contains(vId)
    }).collect().foreach(println)

  }

}
