package com.becomedataengineer.exercise1

import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp
import scala.collection.mutable


object VisitsMapper {

  case class VisitState(firstEventTimeEpochMillis: Long,
                        pages: Seq[Visit], browserCode: String, browserVersion: String) {
    def toVisitOutput(userId: Int): VisitOutput = {
      val sortedVisits = pages.sortBy(page => page.time.getTime)
      val firstVisit = sortedVisits.head
      val sessionId = s"${userId}_${firstEventTimeEpochMillis}"

      val navigation = new mutable.ListBuffer[VisitTimeSpentAndPage]()
      for (i <- 0 until sortedVisits.size - 1) {
        val currentVisit = sortedVisits(i)
        val nextVisit = sortedVisits(i + 1)
        val timeSpent = nextVisit.time.getTime -
          currentVisit.time.getTime
        navigation.append(VisitTimeSpentAndPage(currentVisit.page, timeSpent))
      }
      navigation.append(VisitTimeSpentAndPage(sortedVisits.last.page, 0L))
      VisitOutput(
        sessionId = sessionId, userId = userId, startTime = firstVisit.time,
        endTime = sortedVisits.last.time, browserCode = browserCode, browserVersion = browserVersion,
        navigation = navigation
      )
    }
  }
  case class Visit(page: String, time: Timestamp) {

  }
  private object Visit {
    def fromUserVisitWithTimestamp(rawVisit: UserVisitWithTimestamp): Visit = {
      Visit(rawVisit.visitedPage, rawVisit.timestamp)
    }
  }

  def generateVisitDuration(timeoutDurationMs: Long)(userId: Int, visitEvents: Iterator[UserVisitWithTimestamp],
                                                     currentState: GroupState[VisitState]): Option[VisitOutput] = {
    if (currentState.hasTimedOut) {
      println(s"Session (${currentState.get}) expired for ${userId}; let's generate the final output here")
      val visitDuration = currentState.get.toVisitOutput(userId)
      currentState.remove()
      Some(visitDuration)
    } else {
      val stateToUpdate = if (currentState.exists) {
        currentState.get.copy(
          pages = currentState.get.pages ++ visitEvents.map(visit => Visit.fromUserVisitWithTimestamp(visit))
        )
      } else {
        var browserCode = ""
        var browserVersion = ""
        var firstEventTime = Long.MaxValue
        val newPages = visitEvents.map(visit => {
          browserCode = visit.browserKey
          browserVersion = visit.browserVersion
          firstEventTime = Math.min(firstEventTime, visit.timestamp.getTime)
          Visit(visit.visitedPage, visit.eventTime)
        }).toSeq
        VisitState(
          firstEventTimeEpochMillis = firstEventTime, pages = newPages,
          browserCode = browserCode, browserVersion = browserVersion
        )
      }
      currentState.update(stateToUpdate)

      val baseWatermark: Long = if (currentState.getCurrentWatermarkMs() > 0L) {
        currentState.getCurrentWatermarkMs()
      } else {
        stateToUpdate.pages.maxBy(visit => visit.time.getTime).time.getTime
      }
      currentState.setTimeoutTimestamp(baseWatermark + timeoutDurationMs)
      None
    }
  }
}

case class VisitOutput(sessionId: String, userId: Int, startTime: Timestamp, endTime: Timestamp,
                       browserCode: String, browserVersion: String,
                       navigation: Seq[VisitTimeSpentAndPage])

case class VisitTimeSpentAndPage(page: String, timeSpent: Long)