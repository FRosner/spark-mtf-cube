package de.frosner.spark.mtf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import scodec.bits.{BitVector, ByteOrdering}
import scodec.{Codec, DecodeResult, codecs => Codecs}
import scodec.Attempt.{Failure, Successful}
import MtfCubeRelation._
import org.apache.hadoop.conf.Configuration

case class MtfCubeRelation(location: String,
                           times: IndexedSeq[String],
                           instruments: IndexedSeq[String],
                           scenarios: IndexedSeq[String],
                           endianType: ByteOrdering,
                           valueType: DataType)
                     (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {

  require(times.nonEmpty, "'times' must not be empty")
  require(instruments.nonEmpty, "'numInstruments' must not be empty")
  require(scenarios.nonEmpty, "'numScenarios' must not be empty")
  require(
    valueType == DoubleType || valueType == FloatType,
    "Currently only double (8 byte) and float (4 byte) encoding is supported."
  )

  @transient val dataLocation = location + "/cube.dat.*"

//  "INSTRUMENT" "SCENARIO" "WEIGHT" "SIMULATION DATE" "VALUE"
  override def schema: StructType = {
    StructType(Seq(
      StructField("Time", StringType, nullable = false),
      StructField("Instrument", StringType, nullable = false),
      StructField("Scenario", StringType, nullable = false),
      StructField("Value", valueType, nullable = false)
    ))
  }

  override def buildScan(): RDD[Row] = {
    val sparkContext = sqlContext.sparkContext
    if (valueType == FloatType) {
      val recordWidth = 4
      val byteRecords = sparkContext.binaryRecords(dataLocation, recordWidth, sparkContext.hadoopConfiguration)
      val codec = if (endianType == ByteOrdering.LittleEndian) SerializableCodec.FloatL else SerializableCodec.Float
      val values = byteRecords.map(decodeBytes[Float](codec))
      convertValuesToDf(values, times, instruments, scenarios)
    } else if (valueType == DoubleType) {
      val recordWidth = 8
      val byteRecords = sparkContext.binaryRecords(dataLocation, recordWidth, sparkContext.hadoopConfiguration)
      val codec = if (endianType == ByteOrdering.LittleEndian) SerializableCodec.DoubleL else SerializableCodec.Double
      val values = byteRecords.map(decodeBytes[Double](codec))
      convertValuesToDf(values, times, instruments, scenarios)
    } else {
      throw new IllegalStateException(s"Unexpected value type: $valueType")
    }
  }

}

object MtfCubeRelation {

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
                           instruments: IndexedSeq[String],
                           scenarios: IndexedSeq[String]): RDD[Row] = {
    val numTimes = times.size
    val numInstruments = instruments.size
    val numScenarios = scenarios.size
    val valuesWithIndex = values.zipWithIndex()
    val rows = valuesWithIndex.map {
      case (value, index) =>
        val timeIndex = (index / (numInstruments * numScenarios) % numTimes).toInt
        val instrumentIndex = (index / numScenarios % numInstruments).toInt
        val scenarioIndex = (index % numScenarios).toInt
        Row.fromSeq(Seq(times(timeIndex), instruments(instrumentIndex), scenarios(scenarioIndex), value))
    }
    rows
  }

}