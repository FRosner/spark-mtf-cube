package de.frosner.spark.mtf

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}
import org.scalatest.{FlatSpec, Matchers}
import scodec.bits.ByteOrdering
import scodec.{codecs => Codecs}

class MtfCubeRelationSpec extends FlatSpec with Matchers {

  "Decoding bytes" should "work" in {
    val codec = SerializableCodec.Float
    val value = 4f
    val valueBytes = Codecs.float.encode(value).toOption.get.toByteArray
    MtfCubeRelation.decodeBytes[Float](codec)(valueBytes) shouldBe value
  }

  it should "fail if it tries to decode something which is not correct" in {
    val codec = SerializableCodec.Float
    val valueBytes = Array(2.toByte)
    intercept[DecodingFailedException] {
      MtfCubeRelation.decodeBytes[Float](codec)(valueBytes)
    }
  }

  it should "fail if it tries to decode something which has a remainder" in {
    val codec = SerializableCodec.Float
    val valueBytes = Array.fill(5)(2.toByte)
    intercept[NonEmptyRemainderException] {
      MtfCubeRelation.decodeBytes[Float](codec)(valueBytes)
    }
  }

  "Build scan" should "work" in {
    val relation = MtfCubeRelation(
      location = "src/test/resources/small",
      times = IndexedSeq("1"),
      simulatableDimensions = IndexedSeq(Left(Instrument("1", "t", "c"))),
      scenarios = IndexedSeq("1"),
      baseCurrency = "EUR",
      endianType = ByteOrdering.LittleEndian,
      valueType = FloatType,
      checkCubeSize = false
    )(SparkSession.builder().master("local").getOrCreate().sqlContext)
    relation.buildScan().collect shouldBe Array(
      Row.fromSeq(Seq("1", "EUR", Row.fromSeq(Seq("1", "t", "c")), null, "1", 0f))
    )
  }

}
