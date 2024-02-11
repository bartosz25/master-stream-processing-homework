package com.becomedataengineer.exercise1.part1.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonMapper {

  val Instance = new ObjectMapper()
  Instance.registerModule(DefaultScalaModule)
  Instance.registerModule(new JavaTimeModule())

}
