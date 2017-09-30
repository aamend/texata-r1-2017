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

package com.aamend.texata.api

import org.musicbrainz.controller._
import org.musicbrainz.model.entity.ArtistWs2

import scala.collection.JavaConversions._

class MusicBrainz extends Serializable {

  val searchRelease: Unit = {
    val release = new Release()
    release.search("date:1990-??-?? AND creditname:pink floyd")
    val results = release.getFirstSearchResultPage
    for (rec <- results) {
      System.out.println(rec.getRelease.toString)
    }
  }

  def searchISRC(): Unit = {
    val isrc = "GBAYE6900522"
    val recording = new Recording()
    recording.search("isrc:" + isrc)
    val results = recording.getFullSearchResultList
    for (rec <- results) {
      System.out.println(rec.getRecording.toString)
    }
  }

  def searchISWC(): Unit = {
    val iswc = "T-010.475.727-8"
    val work = new Work()
    work.getIncludes.setTags(true)
    work.getIncludes.setArtistCredits(true)
    work.search("iswc:" + iswc)
    val results = work.getFirstSearchResultPage.head.getWork
    val w = work.lookUp(results)
    println(w.getArtistCredit)
    println(w.getTitle)
    println(w.getWritersString)
    w.getTags.foreach(println)
    println(w.getTags.length)
  }

  def searchArtist(): Unit = {
    val artist = new Artist()
    artist.search("Run the Jewels")
    val results = artist.getFirstSearchResultPage
    val pf = results.get(0).getArtist
    val pinkFloyd = new Artist()

    pinkFloyd.getIncludes.setArtistRelations(true)
    pinkFloyd.getIncludes.setLabelRelations(true)
    pinkFloyd.getIncludes.setWorkRelations(true)

    pinkFloyd.getIncludes.setAliases(false)
    pinkFloyd.getIncludes.setAnnotation(false)
    pinkFloyd.getIncludes.setReleaseGroups(false)
    pinkFloyd.getIncludes.setReleases(false)
    pinkFloyd.getIncludes.setRatings(true)
    pinkFloyd.getIncludes.setRecordings(false)
    pinkFloyd.getIncludes.setVariousArtists(false)
    pinkFloyd.getIncludes.setWorks(false)

    val s: ArtistWs2 = pinkFloyd.getComplete(pf)

    println(s.getRating.getAverageRating)
    println(s.getRating.getVotesCount)

    //TODO: Recommend new song from same artist. Surely this crazy gamer would be fond of a new release
  }

  def searchRelations(): Unit = {
    val artist = new Artist()
    artist.search("pink floyd")
    val results = artist.getFirstSearchResultPage

    val dp = new Artist()
    dp.getIncludes.setArtistRelations(true)
    val daftPunk = dp.lookUp(results.get(0).getArtist)
    daftPunk.getRelationList.getRelations.foreach(re => {
      println(re.getTarget.toString + " : " + re.getDirection)
    })
  }

  def searchRelations2(): Unit = {
    val artist = new Artist()
    artist.search("pink floyd")
    val results = artist.getFirstSearchResultPage

    val dp = new Artist()
    dp.getIncludes.setArtistRelations(true)
    val daftPunk = dp.lookUp(results.get(0).getArtist)
    daftPunk.getRelationList.getRelations.foreach(re => {
      println(re.getTarget.toString + " : " + re.getDirection)
    })
  }
}