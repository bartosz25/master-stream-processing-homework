package com.becomedataengineer.exercise1

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.util.UUID
import scala.util.Random

case class UserVisit(eventId: String = UUID.randomUUID().toString,
                     os: String = Random.shuffle(List("macOS", "Windows", "Linux")).head,
                     visitedPage: String = Random.shuffle(List(
                       "category-1", "category-1/product-1", "index.html", "contact.html"
                     )).head) {

  def asJson: String = UserVisit.ScalaJsonMapper.writeValueAsString(this)

}

object UserVisit {

  val ScalaJsonMapper = new ObjectMapper()
  ScalaJsonMapper.registerModule(DefaultScalaModule)
  ScalaJsonMapper.registerModule(new JavaTimeModule())

}