package de.frosner.spark.mtf

import de.frosner.spark.mtf.DefaultSource._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.scalatest.{FlatSpec, Matchers}
import scodec.bits.ByteOrdering

class DefaultSourceSpec extends FlatSpec with Matchers {

  "The DefaultSource" should "load a single small file containing one record correctly" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("de.frosner.spark.mtf")
        .option(DefaultSource.NumTimesKey, "1")
        .option(DefaultSource.NumInstrumentsKey, "1")
        .option(DefaultSource.NumScenariosKey, "1")
        .option(DefaultSource.EndianTypeKey, "LittleEndian")
        .option(DefaultSource.ValueTypeKey, "FloatType")
        .load("src/test/resources/small")
    df.show()
    println(df.rdd.partitions.mkString("\n"))
    df.count() shouldBe 1
  }

  it should "read multiple files correctly" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("de.frosner.spark.mtf")
      .option(DefaultSource.NumTimesKey, "1")
      .option(DefaultSource.NumInstrumentsKey, "1")
      .option(DefaultSource.NumScenariosKey, "1")
      .option(DefaultSource.EndianTypeKey, "LittleEndian")
      .option(DefaultSource.ValueTypeKey, "FloatType")
      .load("src/test/resources/multifile")
    df.show()
    println(df.rdd.partitions.mkString("\n"))
    df.count() shouldBe 4
  }

  "Long input parameter validation" should "validate correctly" in {
    val parameter = "param"
    val parameters = Map(parameter -> "5")
    DefaultSource.validateAndGetLong(parameters, parameter) shouldBe 5L
  }

  it should "throw an exception if the parameter is not present" in {
    val parameter = "param"
    val parameters = Map.empty[String, String]
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetLong(parameters, parameter)
    }
  }

  it should "throw an exception if the value is not a long" in {
    val parameter = "param"
    val parameters = Map(parameter -> "dasda")
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetLong(parameters, parameter)
    }
  }

  "Byte order input parameter validation" should "validate little endian correctly" in {
    val parameters = Map(EndianTypeKey -> "LittleEndian")
    DefaultSource.validateAndGetEndianType(parameters) shouldBe ByteOrdering.LittleEndian
  }

  it should "validate big endian correctly" in {
    val parameters = Map(EndianTypeKey -> "BigEndian")
    DefaultSource.validateAndGetEndianType(parameters) shouldBe ByteOrdering.BigEndian
  }

  it should "throw an exception if the parameter is not present" in {
    val parameters = Map.empty[String, String]
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetEndianType(parameters)
    }
  }

  it should "throw an exception if the value is not a valid byte ordering" in {
    val parameters = Map(EndianTypeKey -> "dasda")
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetEndianType(parameters)
    }
  }

  "Value type input parameter validation" should "validate float correctly" in {
    val parameters = Map(ValueTypeKey -> "FloatType")
    DefaultSource.validateAndGetValueType(parameters) shouldBe FloatType
  }

  it should "validate double correctly" in {
    val parameters = Map(ValueTypeKey -> "DoubleType")
    DefaultSource.validateAndGetValueType(parameters) shouldBe DoubleType
  }

  it should "throw an exception if the parameter is not present" in {
    val parameters = Map.empty[String, String]
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetValueType(parameters)
    }
  }

  it should "throw an exception if the value is not a valid value type" in {
    val parameters = Map(ValueTypeKey -> "dasda")
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetValueType(parameters)
    }
  }

}
