package com.becomedataengineer.exercise1

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp

case class UserVisitWithTimestamp(userId: Int, eventTime: Timestamp, browserKey: String,
                     eventId: String,
                     browserVersion: String,
                     visitedPage: String,
                     timestamp: Timestamp) {

  def asJson: String = UserVisitWithTimestamp.ScalaJsonMapper.writeValueAsString(this)

}

object UserVisitWithTimestamp {

  val Pages: List[String] = List(
    "category-1", "category-1/product-1", "index.html", "contact.html"
  )

  val ScalaJsonMapper = new ObjectMapper()
  ScalaJsonMapper.registerModule(DefaultScalaModule)
  ScalaJsonMapper.registerModule(new JavaTimeModule())

  val SchemaAsStructType: StructType =
    StructType(ScalaReflection.schemaFor[UserVisitWithTimestamp].dataType.asInstanceOf[StructType]
      .filter(field => field.name != "timestamp"))

}