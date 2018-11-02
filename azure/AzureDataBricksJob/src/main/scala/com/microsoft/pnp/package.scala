package com.microsoft

import org.apache.spark.sql.types._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils.secrets

package object pnp {

  def getSecret(secretScope: String, secretName: String): String = {
    secrets.get(secretScope, secretName)
  }

  val RideSchema = new StructType()
    .add("rateCode", IntegerType)
    .add("storeAndForwardFlag", StringType)
    .add("dropoffTime", TimestampType)
    .add("passengerCount", IntegerType)
    .add("tripTimeInSeconds", DoubleType)
    .add("tripDistanceInMiles", DoubleType)
    .add("pickupLon", DoubleType)
    .add("pickupLat", DoubleType)
    .add("dropoffLon", DoubleType)
    .add("dropoffLat", DoubleType)
    .add("medallion", LongType)
    .add("hackLicense", LongType)
    .add("vendorId", StringType)
    .add("pickupTime", TimestampType)
    .add("errorMessage", StringType)
    .add("messageData", StringType)

  val FareSchema = new StructType()
    .add("medallion", LongType)
    .add("hackLicense", LongType)
    .add("vendorId",StringType)
    .add("pickupTimeString", StringType)
    .add("paymentType", StringType)
    .add("fareAmount", DoubleType)
    .add("surcharge", DoubleType)
    .add("mtaTax", DoubleType)
    .add("tipAmount", DoubleType)
    .add("tollsAmount", DoubleType)
    .add("totalAmount", DoubleType)
}

