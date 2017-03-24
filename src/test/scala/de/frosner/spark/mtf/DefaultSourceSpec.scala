package de.frosner.spark.mtf

import de.frosner.spark.mtf.DefaultSource._
import org.apache.spark.sql.{Row, SparkSession}
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
        .load("src/test/resources/small/cube.dat.0")
    df.count() shouldBe 1
  }

  it should "read multiple files correctly" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("de.frosner.spark.mtf")
      .option(DefaultSource.NumTimesKey, "2")
      .option(DefaultSource.NumInstrumentsKey, "1")
      .option(DefaultSource.NumScenariosKey, "2")
      .option(DefaultSource.EndianTypeKey, "LittleEndian")
      .option(DefaultSource.ValueTypeKey, "FloatType")
      .load("src/test/resources/multifile/cube.dat.*")
    df.count() shouldBe 4
  }

  it should "assign the dimensions correctly" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("de.frosner.spark.mtf")
      .option(DefaultSource.NumTimesKey, "4")
      .option(DefaultSource.NumInstrumentsKey, "2")
      .option(DefaultSource.NumScenariosKey, "2")
      .option(DefaultSource.EndianTypeKey, "BigEndian")
      .option(DefaultSource.ValueTypeKey, "DoubleType")
      .load("src/test/resources/big/cube.dat.0")
    val instrument1 = Row.fromSeq(Seq("0", "t", "cur"))
    val instrument2 = Row.fromSeq(Seq("1", "t", "cur"))
    val baseCurrency = "EUR"
    val result = df.select("Time", "BaseCurrency", "Instrument", "Scenario").collect()
    result shouldBe Array(
      Row.fromSeq(Seq("0", baseCurrency, instrument1, "0")),
      Row.fromSeq(Seq("0", baseCurrency, instrument1, "1")),
      Row.fromSeq(Seq("0", baseCurrency, instrument2, "0")),
      Row.fromSeq(Seq("0", baseCurrency, instrument2, "1")),
      Row.fromSeq(Seq("1", baseCurrency, instrument1, "0")),
      Row.fromSeq(Seq("1", baseCurrency, instrument1, "1")),
      Row.fromSeq(Seq("1", baseCurrency, instrument2, "0")),
      Row.fromSeq(Seq("1", baseCurrency, instrument2, "1")),
      Row.fromSeq(Seq("2", baseCurrency, instrument1, "0")),
      Row.fromSeq(Seq("2", baseCurrency, instrument1, "1")),
      Row.fromSeq(Seq("2", baseCurrency, instrument2, "0")),
      Row.fromSeq(Seq("2", baseCurrency, instrument2, "1")),
      Row.fromSeq(Seq("3", baseCurrency, instrument1, "0")),
      Row.fromSeq(Seq("3", baseCurrency, instrument1, "1")),
      Row.fromSeq(Seq("3", baseCurrency, instrument2, "0")),
      Row.fromSeq(Seq("3", baseCurrency, instrument2, "1"))
    )
  }

  it should "fail if the number of records read does not match the expected cube size" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    intercept[InvalidCubeSizeException] {
      spark.read.format("de.frosner.spark.mtf")
        .option(DefaultSource.NumTimesKey, "2")
        .option(DefaultSource.NumInstrumentsKey, "1")
        .option(DefaultSource.NumScenariosKey, "1")
        .option(DefaultSource.EndianTypeKey, "LittleEndian")
        .option(DefaultSource.ValueTypeKey, "FloatType")
        .option(DefaultSource.CheckCubeKey, "true")
        .load("src/test/resources/small/cube.dat.0").count
    }
  }

  it should "not fail if the number of records read does not match the expected cube size but the check is disabled" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    val cube = spark.read.format("de.frosner.spark.mtf")
      .option(DefaultSource.NumTimesKey, "2")
      .option(DefaultSource.NumInstrumentsKey, "1")
      .option(DefaultSource.NumScenariosKey, "1")
      .option(DefaultSource.EndianTypeKey, "LittleEndian")
      .option(DefaultSource.ValueTypeKey, "FloatType")
      .option(DefaultSource.CheckCubeKey, "false")
      .load("src/test/resources/small/cube.dat.0")
    cube.count shouldBe 1
  }

  "Reading the meta data XML" should "work if there are only instruments" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("de.frosner.spark.mtf")
      .option("csrFile", "src/test/resources/withxmlonlyinstruments/cube.csr")
      .load("src/test/resources/withxmlonlyinstruments/cube.dat.0")
    val scenario1 = "Base Scenario"
    val scenario2 = "MC_1"
    val scenario3 = "MC_2"
    val scenario4 = "MC_3"
    val scenario5 = "MC_4"
    val instrument1 = Row.fromSeq(Seq("Instrument 1", "Type A", "EUR"))
    val instrument2 = Row.fromSeq(Seq("Instrument 2", "Type A", "EUR"))
    val instrument3 = Row.fromSeq(Seq("Instrument 3", "Type B", "EUR"))
    val time1 = "2000/01/01 (0)"
    val baseCurrency = "EUR"
    df.collect shouldBe Array(
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario3, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario4, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario5, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario3, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario4, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario5, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument3, null, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument3, null, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument3, null, scenario3, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument3, null, scenario4, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument3, null, scenario5, 0f))
    )
  }

  it should "work if there are instruments and currencies" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("de.frosner.spark.mtf")
      .option("csrFile", "src/test/resources/withxml/cube.csr")
      .load("src/test/resources/withxmlonlyinstruments/cube.dat.0")
    val scenario1 = "Base Scenario"
    val scenario2 = "MC_1"
    val scenario3 = "MC_2"
    val instrument1 = Row.fromSeq(Seq("Instrument 1", "Type A", "EUR"))
    val instrument2 = Row.fromSeq(Seq("Instrument 2", "Type A", "EUR"))
    val baseCurrency = "EUR"
    val currency1 = "CUR1"
    val currency2 = "CUR2"
    val currency3 = "CUR3"
    val time1 = "2000/01/01 (0)"
    df.collect shouldBe Array(
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument1, null, scenario3, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, instrument2, null, scenario3, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency1, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency1, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency1, scenario3, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency2, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency2, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency2, scenario3, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency3, scenario1, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency3, scenario2, 0f)),
      Row.fromSeq(Seq(time1, baseCurrency, null, currency3, scenario3, 0f))
    )
  }

  it should "throw an error if the metadata cannot be read" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    intercept[InvalidMetaDataException] {
      spark.read.format("de.frosner.spark.mtf")
        .option("csrFile", "src/test/resources/withxml/cube.csr.notexisting")
        .load("src/test/resources/withxml/cube.dat.0").count()
    }
  }

  "Long input parameter validation" should "validate correctly" in {
    val parameter = "param"
    val parameters = Map(parameter -> "5")
    DefaultSource.validateAndGetFromInt(parameters, parameter) shouldBe IndexedSeq("0", "1", "2", "3", "4")
  }

  it should "throw an exception if the parameter is not present" in {
    val parameter = "param"
    val parameters = Map.empty[String, String]
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetFromInt(parameters, parameter)
    }
  }

  it should "throw an exception if the value is not a long" in {
    val parameter = "param"
    val parameters = Map(parameter -> "dasda")
    intercept[IllegalArgumentException] {
      DefaultSource.validateAndGetFromInt(parameters, parameter)
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
