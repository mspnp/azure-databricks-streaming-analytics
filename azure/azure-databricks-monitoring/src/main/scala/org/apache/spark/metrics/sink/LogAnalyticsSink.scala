package org.apache.spark.metrics.sink

import java.util.Properties

import com.codahale.metrics.MetricRegistry
import io.dropwizard.metrics.loganalytics.LogAnalyticsReporter
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.{LogAnalyticsSinkConfiguration, SecurityManager}

private class LogAnalyticsSink(
                                 val property: Properties,
                                 val registry: MetricRegistry,
                                 securityMgr: SecurityManager)
  extends Sink with Logging {

  private val config = new LogAnalyticsSinkConfiguration(property)

  MetricsSystem.checkMinimalPollingPeriod(config.pollUnit, config.pollPeriod)

  var reporter = LogAnalyticsReporter.forRegistry(registry)
    .withWorkspaceId(config.workspaceId)
    .withWorkspaceKey(config.workspaceKey)
    .withLogType(config.logType)
    .build()

  override def start(): Unit = {
    reporter.start(config.pollPeriod, config.pollUnit)
    logInfo(s"LogAnalyticsSink started with workspaceId: '${config.workspaceId}'")
  }

  override def stop(): Unit = {
    reporter.stop()
    logInfo("LogAnalyticsSink stopped.")
  }

  override def report(): Unit = reporter.report()
}
