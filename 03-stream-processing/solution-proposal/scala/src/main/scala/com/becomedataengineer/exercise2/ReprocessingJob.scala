package com.becomedataengineer.exercise2

import com.becomedataengineer.exercise2.SinkConfiguration.SinkOptions
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object ReprocessingJob {

  def main(args: Array[String]): Unit = {
    val firstDate = LocalDate.parse(args(0))
    val lastDate = LocalDate.parse(args(1))
    var partitionDate = firstDate
    val partitionDates = new mutable.ListBuffer[String]()
    while (partitionDate.isBefore(lastDate.plusDays(1))) {
      partitionDates.append(partitionDate.format(DateTimeFormatter.ISO_DATE))
      partitionDate = partitionDate.plusDays(1)
    }
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()

    val inputDataFrames = partitionDates.map(eventTime => {
      sparkSession.read.schema("key STRING, value STRING").json(s"${FilesOutputDir}/event_time=${eventTime}")
    })

    val allDataToReprocess = inputDataFrames
      .reduce((partitionsSoFar, newPartition) => partitionsSoFar.union(newPartition))

    val recordsToWrite = StreamingJobProcessingLogic.processInputData(allDataToReprocess)

    recordsToWrite.write.format("kafka").options(SinkOptions).save()
  }

}
