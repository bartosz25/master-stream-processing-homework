package com.becomedataengineer.dispatcher.model

import java.sql.Timestamp

case class CleansedKeyWithValue(key: String, value: CleansedEventLog)

case class CleansedEventLog(visit_id: String, user_id: Long, event_time: Timestamp, page: Page, source: Source,
                            user: Option[User], technical: CleansedTechnical,
                            keep_private: Boolean)
case class CleansedDevice(`type`: String, version: Option[String], full_name: Option[String] = None)
case class CleansedTechnical(browser: String, os: String, lang: String, network: String, device: Option[CleansedDevice])
