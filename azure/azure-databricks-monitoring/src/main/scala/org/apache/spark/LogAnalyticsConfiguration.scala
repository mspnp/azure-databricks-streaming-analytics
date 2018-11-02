package org.apache.spark

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging

private[spark] trait LogAnalyticsConfiguration extends Logging {
  protected def getWorkspaceId: Option[String]
  protected def getWorkspaceKey: Option[String]
  protected def getLogType: String
  protected def getTimestampFieldName: Option[String]

  val workspaceId: String = {
    val value = getWorkspaceId
    require (value.isDefined, "A Log Analytics Workspace ID is required")
    logInfo(s"Setting workspaceId to ${value.get}")
    value.get
  }

  val workspaceKey: String = {
    val value = getWorkspaceKey
    require(value.isDefined, "A Log Analytics Workspace Key is required")
    value.get
  }


  val logType: String = {
    val value = getLogType
    logInfo(s"Setting logType to $value")
    value
  }

  val timestampFieldName: String = {
    val value = getTimestampFieldName
    logInfo(s"Setting timestampNameField to $value")
    value.orNull
  }
}

private[spark] object LogAnalyticsSinkConfiguration {
  private[spark] val LOGANALYTICS_KEY_WORKSPACEID = "workspaceId"
  private[spark] val LOGANALYTICS_KEY_SECRET = "secret"
  private[spark] val LOGANALYTICS_KEY_LOGTYPE = "logType"
  private[spark] val LOGANALYTICS_KEY_TIMESTAMPFIELD = "timestampField"
  private[spark] val LOGANALYTICS_KEY_PERIOD = "period"
  private[spark] val LOGANALYTICS_KEY_UNIT = "unit"

  private[spark] val LOGANALYTICS_DEFAULT_LOGTYPE = "SparkMetric"
  private[spark] val LOGANALYTICS_DEFAULT_PERIOD = "10"
  private[spark] val LOGANALYTICS_DEFAULT_UNIT = "SECONDS"
}

private[spark] class LogAnalyticsSinkConfiguration(properties: Properties)
  extends LogAnalyticsConfiguration {

  import LogAnalyticsSinkConfiguration._

  override def getWorkspaceId: Option[String] = Option(properties.getProperty(LOGANALYTICS_KEY_WORKSPACEID))

  override def getWorkspaceKey: Option[String] = Option(properties.getProperty(LOGANALYTICS_KEY_SECRET))

  override def getLogType: String = properties.getProperty(LOGANALYTICS_KEY_LOGTYPE, LOGANALYTICS_DEFAULT_LOGTYPE)

  override def getTimestampFieldName: Option[String] = Option(properties.getProperty(LOGANALYTICS_KEY_TIMESTAMPFIELD, null))

  val pollPeriod: Int = {
    val value = properties.getProperty(LOGANALYTICS_KEY_PERIOD, LOGANALYTICS_DEFAULT_PERIOD).toInt
    logInfo(s"Setting polling period to $value")
    value
  }

  val pollUnit: TimeUnit = {
    val value = TimeUnit.valueOf(
      properties.getProperty(LOGANALYTICS_KEY_UNIT, LOGANALYTICS_DEFAULT_UNIT).toUpperCase)
    logInfo(s"Setting polling unit to $value")
    value
  }
}
