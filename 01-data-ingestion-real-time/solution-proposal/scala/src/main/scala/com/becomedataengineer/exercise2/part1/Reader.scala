package com.becomedataengineer.exercise2.part1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Reader extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  import sparkSession.implicits._

  println("--- show results ---")
  sparkSession.read.format("delta").load("/tmp/bde-3/homework/p2/e1/output/valid")
    .show(false)
}