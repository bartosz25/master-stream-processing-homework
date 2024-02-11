package com.becomedataengineer.exercise1

import com.becomedataengineer.exercise1.UserVisit.Pages
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import java.time.LocalDateTime
import java.util.UUID
import scala.util.Random

case class UserVisit(userId: Int, eventTime: LocalDateTime, browserKey: String,
                     eventId: String = UUID.randomUUID().toString,
                     browserVersion: String = "1.11",
                     visitedPage: String = Random.shuffle(Pages).head) {

}

object UserVisit {

  val Pages: List[String] = List(
    "category-1", "category-1/product-1", "index.html", "contact.html"
  )

  val SchemaAsStructType: StructType = ScalaReflection.schemaFor[UserVisit].dataType.asInstanceOf[StructType]

}