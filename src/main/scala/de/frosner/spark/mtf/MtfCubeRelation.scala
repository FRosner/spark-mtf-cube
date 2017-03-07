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
                      numTime: Long,
                      numInstruments: Long,
                      numScenarious: Long,
                      endianType: ByteOrdering,
                      valueType: DataType)
                     (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {

  require(numTime > 0, "'numTime' must be positive")
  require(numInstruments > 0, "'numInstruments' must be positive")
  require(numScenarious > 0, "'numScenarious' must be positive")
  require(
    valueType == DoubleType || valueType == FloatType,
    "Currently only double (8 byte) and float (4 byte) encoding is supported."
  )

  @transient val dataLocation = location + "/cube.dat.*"

  val (recordWidth, codec) = if (valueType == FloatType) {
    (4, if (endianType == ByteOrdering.LittleEndian) Codecs.floatL else Codecs.float)
  } else if (valueType == DoubleType) {
    (8, if (endianType == ByteOrdering.LittleEndian) Codecs.doubleL else Codecs.double)
  } else {
    throw new IllegalStateException(s"Unexpected value type: $valueType")
  }

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
      convertValuesToDf(values)
    } else if (valueType == DoubleType) {
      val recordWidth = 8
      val byteRecords = sparkContext.binaryRecords(dataLocation, recordWidth, sparkContext.hadoopConfiguration)
      val codec = if (endianType == ByteOrdering.LittleEndian) SerializableCodec.DoubleL else SerializableCodec.Double
      val values = byteRecords.map(decodeBytes[Double](codec))
      convertValuesToDf(values)
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

  def convertValuesToDf[T](values: RDD[T]): RDD[Row] = {
    val valuesWithIndex = values.zipWithIndex()
    val rows = valuesWithIndex.map {
      case (value, index) => Row.fromSeq(Seq(index.toString, "i", "s", value))
    }
    rows
  }

}