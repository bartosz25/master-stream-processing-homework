package com.becomedataengineer.exercise1.part1.generation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.IOUtils
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients
import org.apache.spark.sql.SparkSession

import java.time.{LocalDateTime, ZoneId}
import java.util.UUID

object DataGenerator {

  val ScalaJsonMapper = new ObjectMapper()
  ScalaJsonMapper.registerModule(DefaultScalaModule)
  ScalaJsonMapper.registerModule(new JavaTimeModule())

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val utc = ZoneId.of("UTC")

    val firstDayToGenerate = LocalDateTime.now(utc).minusDays(10)
    var generationTime = firstDayToGenerate
    while (generationTime.isBefore(LocalDateTime.now(utc))) {
      Seq(
        Visit(eventId = UUID.randomUUID().toString, visitId = 1, userId = 1, visitedPage = "/home",
          visitTime = generationTime.toString),
        Visit(eventId = UUID.randomUUID().toString, visitId = 1, userId = 1, visitedPage = "/contact",
          visitTime = generationTime.toString),
        Visit(eventId = UUID.randomUUID().toString, visitId = 1, userId = 1, visitedPage = "/products",
          visitTime = generationTime.toString),
        Visit(eventId = UUID.randomUUID().toString, visitId = 2, userId = 2, visitedPage = "/my-account",
          visitTime = generationTime.toString),
        Visit(eventId = UUID.randomUUID().toString, visitId = 2, userId = 2, visitedPage = "/my-orders",
          visitTime = generationTime.toString),
        Visit(eventId = UUID.randomUUID().toString, visitId = 2, userId = 2, visitedPage = "/my-orders/order/1",
          visitTime = generationTime.toString),
        Visit(eventId = UUID.randomUUID().toString, visitId = 3, userId = 3, visitedPage = "/home",
          visitTime = generationTime.toString)
      ).toDF.as[Visit].coalesce(1).foreachPartition((rows: Iterator[Visit]) => {
        val httpClient = HttpClients.createDefault()
        rows.grouped(3).foreach(visitsGroup => {
          val httpPostRequest = new HttpPost("http://localhost:8088/visits/ingest")
          val entity = new StringEntity(ScalaJsonMapper.writeValueAsString(Map("visits" -> visitsGroup)))
          println(s"Sending ${ScalaJsonMapper.writeValueAsString(Map("visits" -> visitsGroup))}")
          httpPostRequest.setEntity(entity)
          httpPostRequest.setHeader("Accept", "application/json")
          httpPostRequest.setHeader("Content-type", "application/json")
          println(entity)
          val response = httpClient.execute(httpPostRequest)
          if (response.getStatusLine().getStatusCode != 200) {
            println(IOUtils.toString(response.getEntity.getContent, "UTF-8"))
            throw new IllegalStateException(s"Something went wrong. The API responded with ${response.getStatusLine().getStatusCode}")
          }
          httpPostRequest.reset()
        })
      })

      generationTime = generationTime.plusMinutes(1L)
      Thread.sleep(5000L)
    }
  }

}
case class Visit(eventId: String, visitId: Int, userId: Int, visitedPage: String, visitTime: String)