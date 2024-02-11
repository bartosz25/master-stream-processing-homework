package com

import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

package object becomedataengineer {

  case class Visit(visitId: Int, userId: Int, visitedPage: String, visitTime: String)

  val VisitTimestampOnlySchema = StructType(Seq(
    StructField("eventTime", TimestampType)
  ))
}
