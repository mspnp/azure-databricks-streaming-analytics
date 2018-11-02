package com.microsoft.pnp

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

abstract class SparkSuiteBase extends FunSuite {
  lazy val sparkContext = SparkSuiteBase.sparkContext

}

object SparkSuiteBase {
  private val master = "local[*]"
  private val appName = "data_load_testing"
  private lazy val sparkContext: SparkSession = new SparkSession.Builder().appName(appName).master(master).getOrCreate()

}
