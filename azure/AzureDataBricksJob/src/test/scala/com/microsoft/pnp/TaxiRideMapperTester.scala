package com.microsoft.pnp

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.scalatest.{BeforeAndAfterEach, Matchers}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

class TaxiRideMapperTester extends SparkSuiteBase with Matchers with BeforeAndAfterEach {


  val logger = LoggerFactory.getLogger("TaxiRideMapperTester")

  override def beforeEach(): Unit = { }

  override def afterEach() { }

/*
  test("it should parse valid json and match mapJsonToTaxiRide success case") {
    logger.info("it should parse valid json and match mapJsonToTaxiRide success case")
    val taxiRideJsonString = "{\"rateCode\":1,\"storeAndForwardFlag\":\"N\",\"dropoffTime\":\"2013-01-01T00:11:20+00:00\",\"passengerCount\":1,\"tripTimeInSeconds\":413.0,\"tripDistanceInMiles\":2.3,\"pickupLon\":-73.97912,\"pickupLat\":40.7623177,\"dropoffLon\":-73.95027,\"dropoffLat\":40.77126,\"medallion\":2013000717,\"hackLicense\":2013000714,\"vendorId\":\"CMT\",\"pickupTime\":\"2013-01-01T00:04:27+00:00\"}"

    var shouldMapToTaxiRide = false
    TaxiRideMapper.mapJsonToTaxiRide(taxiRideJsonString) match {
      case Success(_) => shouldMapToTaxiRide = true
      case Failure(_) => shouldMapToTaxiRide = false
    }

    assert(shouldMapToTaxiRide)
    println(1)
  }

  test("it should parse corrupted taxi ride json and match mapJsonToTaxiRide failure case") {
    val taxiRideJsonString = "{\"menu\": {\n  \"id\": \"file\",\n  \"value\": \"File\",\n  \"popup\": {\n    \"menuitem\": [\n      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n    ]\n  }\n}}"
    val expected = "0_0_null_null"
    var taxiRide: TaxiRide = null
    TaxiRideMapper.mapJsonToTaxiRide(taxiRideJsonString) match {
      case Success(value) => taxiRide = value
      case Failure(_) =>
    }

    assert(taxiRide.key.contentEquals(expected))
    println(2)
  }

  test("it should parse valid json and match validateJsonString success case") {
    val validJson = "{\n    \"fruit\": \"Apple\",\n    \"size\": \"Large\",\n    \"color\": \"Red\"\n}"

    var shouldValidateToTrue = false
    TaxiRideMapper.validateJsonString(validJson) match {
      case Success(_) => shouldValidateToTrue = true
      case Failure(_) => shouldValidateToTrue = false
    }

    assert(shouldValidateToTrue)
    println(3)
  }

  test("it should parse invalid json and match  validateJsonString failure case") {
    val invalidJson = "some invalid json string"

    var shouldValidateToTrue = false
    TaxiRideMapper.validateJsonString(invalidJson) match {
      case Success(_) => shouldValidateToTrue = true
      case Failure(_) => shouldValidateToTrue = false
    }

    assert(!shouldValidateToTrue)
    println(4)
  }

  test("it should map a valid taxi ride json to a valid enrichedtaxi ride record") {
    val rideContent = "{\"rateCode\":1,\"storeAndForwardFlag\":\"N\",\"dropoffTime\":\"2013-01-01T00:11:20+00:00\",\"passengerCount\":1,\"tripTimeInSeconds\":413.0,\"tripDistanceInMiles\":2.3,\"pickupLon\":-73.97912,\"pickupLat\":40.7623177,\"dropoffLon\":-73.95027,\"dropoffLat\":40.77126,\"medallion\":2013000717,\"hackLicense\":2013000714,\"vendorId\":\"CMT\",\"pickupTime\":\"2013-01-01T00:04:27+00:00\"}"
    val recordIngestedTime = "2018-08-23 12:44:19.818"

    val rideDataFrameSchema = new StructType()
      .add("rideContent", StringType, true)
      .add("recordIngestedTime", TimestampType, true)

    val rideData = Seq(
      Row(rideContent, Timestamp.valueOf(recordIngestedTime))
    )

    import sparkContext.implicits._

    val rideDataFrame = sparkContext.createDataFrame(
      sparkContext.sparkContext.parallelize(rideData),
      rideDataFrameSchema
    )

    val enrichedTaxiRideRecords = rideDataFrame.map(row => TaxiRideMapper.mapRowToEncrichedTaxiRideRecord(row))
      .filter(x => x.isValidRecord).as[EnrichedTaxiDataRecord]

    val expectedCount = 1
    val actualCount = enrichedTaxiRideRecords.count()

    assert(actualCount == expectedCount)
    println(5)
  }

  test("it should map a invalid json string to a invalid enriched taxi ride record") {
    val rideContent = "some invalid json string"
    val recordIngestedTime = "2018-08-23 12:44:19.818"

    val rideDataFrameSchema = new StructType()
      .add("rideContent", StringType, true)
      .add("recordIngestedTime", TimestampType, true)

    import sparkContext.implicits._
    val rideData = Seq(
      Row(rideContent, Timestamp.valueOf(recordIngestedTime))
    )

    val rideDataFrame = sparkContext.createDataFrame(
      sparkContext.sparkContext.parallelize(rideData),
      rideDataFrameSchema
    )

    val enrichedTaxiRideRecords = rideDataFrame.map(row => TaxiRideMapper.mapRowToEncrichedTaxiRideRecord(row))
      .filter(x => x.isValidRecord).as[EnrichedTaxiDataRecord]

    val expectedCount = 0
    val actualCount = enrichedTaxiRideRecords.count()

    assert(actualCount == expectedCount)
    println(6)
  }

  test("it should map a valid json string  but a corrupted taxiride string to a invalid enriched taxi ride record") {
    val rideContent = "{\"menu\": {\n  \"id\": \"file\",\n  \"value\": \"File\",\n  \"popup\": {\n    \"menuitem\": [\n      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n    ]\n  }\n}}"
    val recordIngestedTime = "2018-08-23 12:44:19.818"

    val rideDataFrameSchema = new StructType()
      .add("rideContent", StringType, true)
      .add("recordIngestedTime", TimestampType, true)

    val rideData = Seq(
      Row(rideContent, Timestamp.valueOf(recordIngestedTime))
    )

    import sparkContext.implicits._

    val rideDataFrame = sparkContext.createDataFrame(
      sparkContext.sparkContext.parallelize(rideData),
      rideDataFrameSchema
    )

    val enrichedTaxiRideRecords = rideDataFrame.map(row => TaxiRideMapper.mapRowToEncrichedTaxiRideRecord(row))
      .filter(x => x.isValidRecord).as[EnrichedTaxiDataRecord]

    val expectedCount = 0
    val actualCount = enrichedTaxiRideRecords.count()

    assert(actualCount == expectedCount)
    println(7)
  }
*/
}
