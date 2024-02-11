package com.becomedataengineer.dispatcher.model

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

case class KeyWithValue(key: String, value: EventLog) {
  def toCleansedKeyWithValue: CleansedKeyWithValue = CleansedKeyWithValue(key, value.toCleansedEventLog)

}

case class EventLog(visit_id: String, user_id: Long, event_time: Timestamp, page: Page, source: Source,
                    user: Option[User], technical: Technical, keep_private: Boolean) {


  def toCleansedEventLog: CleansedEventLog = CleansedEventLog(
    visit_id = visit_id, user_id = user_id, event_time = event_time, page = page,
    source = source.toCleansedSource,
    user = user,
    technical = technical.toCleansedTechnical,
    keep_private = keep_private
  )

}

case class Page(current: String, previous: Option[String])
case class Source(site: String, api_version: String) {
  def toCleansedSource = {
    if (site != null && site.startsWith("www.")) {
      this.copy(site = site.drop(4))
    } else {
      this
    }
  }
}
case class User(ip: String, latitude: Double, longitude: Double)
case class Technical(browser: String, os: String, lang: String, network: String, device: Option[Device]) {
  def toCleansedTechnical = {
    val (browserName, language) = cleansedBrowserAndLanguage
    CleansedTechnical(
      browser = browserName, os = os, lang = language, network = cleansedNetwork,
      device = device.map(deviceToClean => deviceToClean.toCleansedDevice)
    )
  }

  private def cleansedNetwork = {
    if (network != null && network.startsWith("{")) {
      val networkStruct = JsonMapper.readValue(network, classOf[NetworkStruct])
      networkStruct.long_name
    } else {
      network
    }
  }

  private def cleansedBrowserAndLanguage = {
    if (browser != null && browser.startsWith("{")) {
      val browserStruct = JsonMapper.readValue(browser, classOf[BrowserStruct])
      (browserStruct.name, browserStruct.language)
    } else {
      (browser, lang)
    }
  }
}
case class NetworkStruct(short_name: String, long_name: String)
case class BrowserStruct(name: String, language: String)
case class Device(`type`: String, version: Option[String]) {
  def toCleansedDevice = {
    val deviceTypeName = if (`type` != null && `type`.startsWith("{")) {
      val deviceStruct = JsonMapper.readValue(`type`, classOf[DeviceTypeStruct])
      deviceStruct.name
    } else {
      `type`
    }
    CleansedDevice(`type` = deviceTypeName, version = version)
  }
}
case class DeviceTypeStruct(name: String)


object EventLog {
  val Schema = ScalaReflection.schemaFor[EventLog].dataType.asInstanceOf[StructType]
}