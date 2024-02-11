package com.becomedataengineer.dispatcher.model

case class KafkaSinkParameters(key: String, value: CleansedEventLog, topic: String)
