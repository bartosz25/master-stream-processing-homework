package com.becomedataengineer.exercise2

object SinkConfiguration {

  val SinkOptions: Map[String, String] = {
    Map("kafka.bootstrap.servers" -> "localhost:29092", "topic" -> "valid_visits")
  }

}
