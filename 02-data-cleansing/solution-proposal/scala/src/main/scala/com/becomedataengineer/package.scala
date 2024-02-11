package com

package object becomedataengineer {

  val BaseDir = "/tmp/bde/module4/homework"

  val CheckpointBaseDir = s"${BaseDir}/checkpoints"

  val KafkaInputConnectionOptions: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "localhost:29092",
    "subscribe" -> "raw_data",
    "startingOffsets" -> "EARLIEST"
  )

  val JdbcReferenceDatasetConnectionOptions: Map[String, String] = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql:bde",
    "dbtable" -> "bde_schema.devices",
    "user" -> "bde_user",
    // Password shouldn't be stored in plain text! Instead you can use a reference name to the
    // values stored in a secrets management store. Here I'm using the plain text only for sake
    // of simplicity
    "password" -> "bde_password"
  )

}
