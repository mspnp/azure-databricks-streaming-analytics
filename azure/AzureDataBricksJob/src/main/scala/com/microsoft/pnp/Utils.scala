package com.microsoft.pnp

import java.time.{ZoneId, ZonedDateTime}
import java.util.HashMap

import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent

object Utils {
  def parsePayload(event: QueryProgressEvent): HashMap[String, AnyRef]={
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))

    val metrics = new HashMap[String, AnyRef]()
    metrics.put("id",  event.progress.id)
    metrics.put("sink", event.progress.sink)
    metrics.put("durationms", event.progress.durationMs.asInstanceOf[AnyRef])
    metrics.put("inputRowsPerSecond", event.progress.inputRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("procRowsPerSecond", event.progress.processedRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("inputRows", event.progress.numInputRows.asInstanceOf[AnyRef])
    metrics.put("DateValue", date.toString)

    metrics
  }
}
