package org.apache.spark.sql.streaming

import java.util.HashMap
import java.util.UUID.randomUUID

import com.microsoft.pnp.{SparkSuiteBase, Utils}
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.scalatest.Matchers

class UtilsTests[sql] extends SparkSuiteBase with Matchers {

  test("should_parse_queryprogress_telemetry") {
    val guid = randomUUID()
    val duration: java.util.Map[String, java.lang.Long] = new HashMap[String, java.lang.Long]
    val eventTime: java.util.Map[String, String] = new HashMap[String, String]

    duration.put("addBatch", 100L)
    duration.put("getBatch", 200L)
    val source: SourceProgress = new SourceProgress("source", "start", "end", 100, 200, 300)
    val sourcearr = new Array[SourceProgress](1)
    sourcearr(0) = source

    val progressEvent = new QueryProgressEvent(
      new StreamingQueryProgress(
        guid, guid,
        "streamTest", "time",
        10, 10, duration,
        eventTime,
        null, sourcearr, null, null
      )
    )

    val metrics = Utils.parsePayload(progressEvent)
    assert(progressEvent.progress.id === metrics.get("id"))
    assert(progressEvent.progress.numInputRows === metrics.get("inputRows"))
    assert(progressEvent.progress.processedRowsPerSecond === metrics.get("procRowsPerSecond"))
    assert(progressEvent.progress.inputRowsPerSecond === metrics.get("inputRowsPerSecond"))
    assert(progressEvent.progress.durationMs.get("addBatch") ===
      metrics.get("durationms").asInstanceOf[HashMap[String, AnyRef]].get("addBatch"))
    assert(progressEvent.progress.durationMs.get("getBatch") ===
      metrics.get("durationms").asInstanceOf[HashMap[String, AnyRef]].get("getBatch"))

  }
}