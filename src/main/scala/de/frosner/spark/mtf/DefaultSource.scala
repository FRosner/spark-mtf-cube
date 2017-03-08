package de.frosner.spark.mtf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{BaseRelation, Filter, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import DefaultSource._
import scodec.bits.ByteOrdering

import scala.util.{Failure, Success, Try}


class DefaultSource extends RelationProvider with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path") match {
      case Some(p) if !p.isEmpty => p
      case other => throw new IllegalArgumentException("'path' must be specified.")
    }
    val csrPath = parameters.get("csrPath")
    if (csrPath.isDefined) {
      ???
    } else {
      val times = validateAndGetFromInt(parameters, NumTimesKey)
      val instruments = validateAndGetFromInt(parameters, NumInstrumentsKey)
      val scenarios = validateAndGetFromInt(parameters, NumScenariosKey)
      val endianType = validateAndGetEndianType(parameters)
      val valueType = validateAndGetValueType(parameters)
      val checkCube = validateAndGetCheckCube(parameters)
      new MtfCubeRelation(path, times, instruments, scenarios, endianType, valueType)(sqlContext)
    }

  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    createRelation(sqlContext, parameters)
  }

}

object DefaultSource {

  val NumTimesKey = "numTimes"
  val NumInstrumentsKey = "numInstruments"
  val NumScenariosKey = "numScenarios"
  val EndianTypeKey = "endianType"
  val ValueTypeKey = "valueType"
  val CheckCubeKey = "checkCube"
  val CheckCubeDefault = false

  def validateAndGetFromInt(parameters: Map[String, String], parameter: String): IndexedSeq[String] = {
    parameters.get(parameter) match {
      case Some(s) =>
        val tryLong = Try(s.toInt).map(i => (0 until i).map(_.toString)).recoverWith{
          case throwable => Failure(new IllegalArgumentException(s"'$parameter' expected to be an integer but got '$s'"))
        }
        tryLong match {
          case Success(l) => l
          case Failure(throwable) => throw throwable
        }
      case other => throw new IllegalArgumentException(s"'$parameter' must be specified.")
    }
  }

  def validateAndGetEndianType(parameters: Map[String, String]): ByteOrdering = {
    val parameter = EndianTypeKey
    parameters.get(parameter) match {
      case Some("LittleEndian") => ByteOrdering.LittleEndian
      case Some("BigEndian") => ByteOrdering.BigEndian
      case Some(other) => throw new IllegalArgumentException(s"'$parameter' expected to be either LittleEndian or BigEndian but got '$other'")
      case other => throw new IllegalArgumentException(s"'$parameter' must be specified.")
    }
  }

  def validateAndGetValueType(parameters: Map[String, String]): DataType = {
    val parameter = ValueTypeKey
    parameters.get(parameter) match {
      case Some("FloatType") => FloatType
      case Some("DoubleType") => DoubleType
      case Some(other) => throw new IllegalArgumentException(s"'$parameter' expected to be either FloatType or DoubleType but got '$other'")
      case other => throw new IllegalArgumentException(s"'$parameter' must be specified.")
    }
  }

}