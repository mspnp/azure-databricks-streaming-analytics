package com.microsoft.pnp

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.slf4j.{Logger, LoggerFactory}

class StreamingMetricsListener() extends StreamingQueryListener {
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  lazy val mdcFactory: MDCCloseableFactory = new MDCCloseableFactory()

  override def onQueryStarted(event: QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    try {
      //parsing the telemetry Payload and logging to ala
      TryWith(this.mdcFactory.create(Utils.parsePayload(event)))(
        c => {
          this.logger.info("onQueryProgress")
        }
      )
    }

    catch {
      case e: Exception => this.logger.error("onQueryProgress", e)
    }
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    if (event.exception.nonEmpty) {
      this.logger.error(event.exception.get)
    }
  }
}
