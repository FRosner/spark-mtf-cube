package de.frosner.spark.mtf

import de.frosner.spark.mtf.MtfCubeRelation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SQLContext}
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteOrdering}
import scodec.{DecodeResult, codecs => Codecs}

case class Instrument(id: String, instrType: String, unit: String)

case class MtfCubeRelation(location: String,
                           times: IndexedSeq[String],
                           simulatableDimensions: IndexedSeq[Either[Instrument, Currency]],
                           scenarios: IndexedSeq[String],
                           baseCurrency: String,
                           endianType: ByteOrdering,
                           valueType: DataType,
                           checkCubeSize: Boolean)
                     (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {

  require(times.nonEmpty, "'times' must not be empty")
  require(simulatableDimensions.nonEmpty, "'numInstruments' must not be empty")
  require(scenarios.nonEmpty, "'numScenarios' must not be empty")
  require(
    valueType == DoubleType || valueType == FloatType,
    "Currently only double (8 byte) and float (4 byte) encoding is supported."
  )

  override def schema: StructType = {
    val instrumentType = StructType(Seq(
      StructField("ID", StringType, nullable = false),
      StructField("InstrType", StringType, nullable = false),
      StructField("Unit", StringType, nullable = false)
    ))
    StructType(Seq(
      StructField("Time", StringType, nullable = false),
      StructField("BaseCurrency", StringType, nullable = false),
      StructField("Instrument", instrumentType, nullable = true),
      StructField("Currency", StringType, nullable = true),
      StructField("Scenario", StringType, nullable = false),
      StructField("Value", valueType, nullable = false)
    ))
  }

  override def buildScan(): RDD[Row] = {
    val sparkContext = sqlContext.sparkContext
    val expectedCubeSize = times.size.toLong * simulatableDimensions.size.toLong * scenarios.size.toLong
    val cube = if (valueType == FloatType) {
      val recordWidth = 4
      val byteRecords = sparkContext.binaryRecords(location, recordWidth, sparkContext.hadoopConfiguration)
      val codec = if (endianType == ByteOrdering.LittleEndian) SerializableCodec.FloatL else SerializableCodec.Float
      val values = byteRecords.map(decodeBytes[Float](codec))
      convertValuesToDf(values, times, simulatableDimensions, scenarios, baseCurrency)
    } else if (valueType == DoubleType) {
      val recordWidth = 8
      val byteRecords = sparkContext.binaryRecords(location, recordWidth, sparkContext.hadoopConfiguration)
      val codec = if (endianType == ByteOrdering.LittleEndian) SerializableCodec.DoubleL else SerializableCodec.Double
      val values = byteRecords.map(decodeBytes[Double](codec))
      convertValuesToDf(values, times, simulatableDimensions, scenarios, baseCurrency)
    } else {
      throw new IllegalStateException(s"Unexpected value type: $valueType")
    }
    if (checkCubeSize) {
      val actualCubeSize = cube.count
      if (actualCubeSize != expectedCubeSize) {
        throw InvalidCubeSizeException(actualCubeSize, expectedCubeSize)
      }
    }
    cube
  }

}

object MtfCubeRelation {

  type Currency = String

  def decodeBytes[T](codec: SerializableCodec)(bytes: Array[Byte]): T = {
    val attempt = codec.unsafeGet[T].decode(BitVector(bytes))
    attempt match {
      case Successful(DecodeResult(value, remainder)) =>
        if (!remainder.isEmpty)
          throw new NonEmptyRemainderException(remainder)
        else
          value
      case Failure(cause) => throw new DecodingFailedException(cause)
    }
  }

  def convertValuesToDf[T](values: RDD[T],
                           times: IndexedSeq[String],
                           simulatableDimensions: IndexedSeq[Either[Instrument, Currency]],
                           scenarios: IndexedSeq[String],
                           baseCurrency: String): RDD[Row] = {
    val numTimes = times.size
    val numSimulatableDimensions = simulatableDimensions.size
    val numScenarios = scenarios.size
    val valuesWithIndex = values.zipWithIndex()
    val instruments = simulatableDimensions.map {
      case Left(instrument) => Row.fromSeq(Seq(
        instrument.id,
        instrument.instrType,
        instrument.unit
      ))
      case Right(_) => null
    }
    val currencies = simulatableDimensions.map {
      case Left(_) => null
      case Right(currency) => currency
    }
    val rows = valuesWithIndex.map {
      case (value, index) =>
        val timeIndex = (index / (numSimulatableDimensions * numScenarios) % numTimes).toInt
        val simulatableDimensionIndex = (index / numScenarios % numSimulatableDimensions).toInt
        val scenarioIndex = (index % numScenarios).toInt
        Row.fromSeq(Seq(
          times(timeIndex),
          baseCurrency,
          instruments(simulatableDimensionIndex),
          currencies(simulatableDimensionIndex),
          scenarios(scenarioIndex),
          value
        ))
    }
    rows
  }

}