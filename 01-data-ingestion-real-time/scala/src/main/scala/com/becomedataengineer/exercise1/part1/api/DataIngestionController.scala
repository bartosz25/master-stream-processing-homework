package com.becomedataengineer.exercise1.part1.api

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.{PostMapping, RequestBody, RequestMapping, RestController}

import javax.servlet.http.HttpServletResponse

@RequestMapping(path = Array("/visits"))
@RestController
class DataIngestionController {

  @PostMapping(path = Array("/ingest"))
  def addUsers(@RequestBody visitsToIngest: VisitsToIngest, response: HttpServletResponse): Unit = {
    println(s"Got visits=${visitsToIngest.visits.mkString(", ")}")
    val kafkaDataProducer = new KafkaDataProducer()
    try {
      kafkaDataProducer.produceDataToKafka(visitsToIngest)
      response.setStatus(HttpStatus.OK.value())
    } catch {
      case e => {
        e.printStackTrace()
        response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value())
      }
    }
    kafkaDataProducer.saveUndeliveredEvents()

    response.getWriter.flush()
    response.getWriter.close()
  }

}
