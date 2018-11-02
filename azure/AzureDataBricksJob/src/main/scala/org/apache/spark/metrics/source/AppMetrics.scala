package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

class AppMetrics(sc: SparkContext) extends Source {
  override val metricRegistry = new MetricRegistry
  override val sourceName = "%s.AppMetrics".format(sc.appName)

  def registerGauge(metricName: String, acc: LongAccumulator) {
    val metric = new Gauge[Long] {
      override def getValue: Long = {
        acc.value
      }
    }
    metricRegistry.register(MetricRegistry.name(metricName), metric)
  }
}
