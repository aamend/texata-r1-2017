package com.aamend.texata

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

trait Harness {

  val config: Config = ConfigFactory.load()
  lazy val greeting: String = Try(config.getString("texata.greeting")).getOrElse("Welcome")

}
