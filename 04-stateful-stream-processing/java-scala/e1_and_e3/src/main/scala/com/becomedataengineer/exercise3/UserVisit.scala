package com.becomedataengineer.exercise3

import com.becomedataengineer.exercise3.UserVisit.{Browsers, Pages}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.util.UUID
import scala.util.Random

case class UserVisit(userId: Int, eventTime: String, eventId: String = UUID.randomUUID().toString,
                     browser: String = Random.shuffle(Browsers).head,
                     visitedPage: String = Random.shuffle(Pages).head) {

  def asJson: String = UserVisit.ScalaJsonMapper.writeValueAsString(this)

}

object UserVisit {

  val Browsers: List[String] = List("Firefox", "Chrome", "Safari")
  val Pages: List[String] = List(
    "category-1", "category-1/product-1", "index.html", "contact.html"
  )

  val ScalaJsonMapper = new ObjectMapper()
  ScalaJsonMapper.registerModule(DefaultScalaModule)
  ScalaJsonMapper.registerModule(new JavaTimeModule())

}