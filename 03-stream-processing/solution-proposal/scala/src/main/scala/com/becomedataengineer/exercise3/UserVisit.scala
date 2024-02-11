package com.becomedataengineer.exercise3

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import java.util.UUID
import scala.util.Random

case class UserVisit(eventId: String = UUID.randomUUID().toString,
                     os: String = Random.shuffle(List("macOS", "Windows", "Linux", "x....", "unknown")).head,
                     visitedPage: Option[String] = Random.shuffle(List(
                       Some("category-1"), Some("category-1/product-1"), Some("index.html"),
                       Some("contact.html"), None
                     )).head,
                     key: String = UUID.randomUUID().toString) {
  def asJson: String = {
    UserVisit.ScalaJsonMapper.writeValueAsString(this)
  }

}

object UserVisit {
  val Schema = ScalaReflection.schemaFor[UserVisit].dataType.asInstanceOf[StructType]

  val ScalaJsonMapper = new ObjectMapper()
  ScalaJsonMapper.registerModule(DefaultScalaModule)
  ScalaJsonMapper.registerModule(new JavaTimeModule())

}