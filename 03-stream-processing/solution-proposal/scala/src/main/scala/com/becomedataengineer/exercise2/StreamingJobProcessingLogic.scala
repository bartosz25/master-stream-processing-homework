package com.becomedataengineer.exercise2

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions}

object StreamingJobProcessingLogic {

  def processInputData(inputDataFrame: DataFrame): DataFrame = {
    val schemaForFiltering = StructType(Array(
      StructField("os", StringType),
      StructField("visitedPage", StringType)
    ))

    import inputDataFrame.sparkSession.implicits._
    inputDataFrame
      .select(
        $"key".cast("string"),
        functions.from_json($"value".cast("string"), schemaForFiltering).as("visit"),
        $"value".cast("string")
      )
      .filter("visit.os IN ('macOS', 'Windows', 'Linux')")
      .filter("visit.visitedPage != ''")
      .select($"key", $"value")
  }

}
