package com.microsoft.pnp

import java.net.URL

import org.rogach.scallop._
import org.apache.spark.sql.catalyst.util.IntervalUtils.stringToInterval
import org.apache.spark.unsafe.types.UTF8String

class JobConfiguration(arguments: Seq[String]) extends ScallopConf(arguments) with Serialization {
  val neighborhoodFileURL = opt[URL](
    name = "neighborhood-file-url",
    short = 'n',
    required = true
  )(urlConverter)

  val taxiRideConsumerGroup = opt[String](default = Some("$Default"))
  val taxiFareConsumerGroup = opt[String](default = Some("$Default"))

  // Intervals
  val windowInterval = opt[String](default = Some("1 hour"), validate = isValidInterval)
  val taxiRideWatermarkInterval = opt[String](default = Some("3 minutes"), validate = isValidInterval)
  val taxiFareWatermarkInterval = opt[String](default = Some("3 minutes"), validate = isValidInterval)

  val secretScope = opt[String](default = Some("azure-databricks-job"))
  val taxiRideEventHubSecretName = opt[String](default = Some("taxi-ride"))
  val taxiFareEventHubSecretName = opt[String](default = Some("taxi-fare"))

  val cassandraHost = opt[String]()

  // cassandra secrets
  val cassandraUserSecretName = opt[String](default = Some("cassandra-username"))
  val cassandraPasswordSecretName = opt[String](default = Some("cassandra-password"))

  verify()

  private def isValidInterval(interval: String): Boolean = {
    // This is the same check spark uses
    val intervalString = if (interval.startsWith("interval")) {
      interval
    } else {
      "interval " + interval
    }
    val cal = stringToInterval(UTF8String.fromString(intervalString))
    cal != null
  }
}
