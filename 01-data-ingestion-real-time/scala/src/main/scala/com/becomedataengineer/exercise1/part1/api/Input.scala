package com.becomedataengineer.exercise1.part1.api

case class VisitsToIngest(visits: Seq[Visit])
case class Visit(eventId: String, visitId: Int, userId: Int, visitedPage: String, visitTime: String)
