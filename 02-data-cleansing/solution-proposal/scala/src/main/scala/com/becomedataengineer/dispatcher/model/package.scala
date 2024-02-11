package com.becomedataengineer.dispatcher

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

package object model {

  val JsonMapper = new ObjectMapper()
  JsonMapper.registerModule(DefaultScalaModule)

}
