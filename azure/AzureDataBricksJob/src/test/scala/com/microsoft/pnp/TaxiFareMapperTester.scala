package com.microsoft.pnp

import org.scalatest.Matchers

import scala.util.{Failure, Success, Try}

class TaxiFareMapperTester extends SparkSuiteBase with Matchers {

/*
  test("should_map_fare_pickup_time_to_ride_pickup_time_format") {
    val expected = "2013-01-01T00:04:27+00:00"
    val inputFarePickUptime = "2013-01-01 00:04:27"
    val actual = TaxiFareMapper.mapFarePickUpTimeToRidePickUpTimeFormat(inputFarePickUptime)
    assert(actual.contentEquals(expected))
  }

  test("csv string with first line comma separated header fields and second line comma separated value fields should be a valid ") {

    val inputString = "header1,header2,header3\nvalue1,value2,value3"

    var shouldSetTrueInSuccessCase = false
    TaxiFareMapper.validateHeaderEmbededCsvString(inputString) match {

      case Success(_) => shouldSetTrueInSuccessCase = true
      case Failure(_) => shouldSetTrueInSuccessCase = false

    }
    assert(shouldSetTrueInSuccessCase)
  }

  test("csv string with only comma separated  value fields  and no header fields should be a invalid ") {
    val inputString = "value1,value2,value3"

    var shouldSetTrueInFailureCase = false
    TaxiFareMapper.validateHeaderEmbededCsvString(inputString) match {
      case Success(_) => shouldSetTrueInFailureCase = false
      case Failure(_) => shouldSetTrueInFailureCase = true
    }
    assert(shouldSetTrueInFailureCase)
  }

  test("csv content with less than 11 fields") {
    val invalidCsvContent = "2013000717,2013000714,CMT,2013-01-01 00:04:27,CRD,8.5,0.5,0.5,2.37,0"
    var shouldSetTrueInFailureCase = false
    var actualErrorMessage = ""

    Try(TaxiFareMapper.mapCsvToTaxiFare(invalidCsvContent)) match {
      case Success(_) => shouldSetTrueInFailureCase = false
      case Failure(exception) =>
        shouldSetTrueInFailureCase = true
        actualErrorMessage = exception.getMessage
    }

    val expectedErrorMessage = TaxiFareMapper.invalidTaxiFareCsv
    assert(shouldSetTrueInFailureCase)
    assert(expectedErrorMessage.contentEquals(actualErrorMessage))
  }
  */
}
