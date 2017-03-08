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
import scala.xml.XML


class DefaultSource extends RelationProvider with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = validateAndGetPath(parameters)
    val checkCube = validateAndGetCheckCube(parameters)
    val csrPathParameter = parameters.get("csrFile")
    csrPathParameter match {
      case Some(csrPath) => createRelationWithMetaDataFile(csrPath, path, checkCube, sqlContext)
      case None => createRelationWithoutMetaDataFile(parameters, path, checkCube, sqlContext)
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

  def validateAndGetPath(parameters: Map[String, String]): String = {
    parameters.get("path") match {
      case Some(p) if !p.isEmpty => p
      case other => throw new IllegalArgumentException("'path' must be specified.")
    }
  }

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

  def validateAndGetCheckCube(parameters: Map[String, String]): Boolean = {
    val parameter = CheckCubeKey
    parameters.get(parameter) match {
      case Some(s) if s.toLowerCase == "true" => true
      case Some(s) if s.toLowerCase == "false" => false
      case Some(other) => throw new IllegalArgumentException(s"'$parameter' expected to be either false or true but got '$other'")
      case None => CheckCubeDefault
    }
  }

  def createRelationWithoutMetaDataFile(parameters: Map[String, String], path: String, checkCube: Boolean, sqlContext: SQLContext): MtfCubeRelation = {
    val times = validateAndGetFromInt(parameters, NumTimesKey)
    val instruments = validateAndGetFromInt(parameters, NumInstrumentsKey)
    val scenarios = validateAndGetFromInt(parameters, NumScenariosKey)
    val endianType = validateAndGetEndianType(parameters)
    val valueType = validateAndGetValueType(parameters)
    new MtfCubeRelation(path, times, instruments, scenarios, endianType, valueType, checkCube)(sqlContext)
  }

  def createRelationWithMetaDataFile(csrPath: String, path: String, checkCube: Boolean, sqlContext: SQLContext): MtfCubeRelation = {
    val tryMetaData = Try {
      XML.loadFile(csrPath)
    }.recoverWith {
      case throwable => Try(XML.loadString(sqlContext.sparkContext.textFile(csrPath).collect().mkString("\n")))
    }.recoverWith {
      case throwable => Failure(InvalidMetaDataException(csrPath, throwable.toString))
    }

    val tryRelation = tryMetaData.flatMap { root =>
      Try {
        val resultInfoProperties = root \ "resultInfo" \ "prop"
        val maybePrecision = resultInfoProperties
          .find(node => (node \ "@name").text == "precision")
          .map(_ \ "@value")
          .map(_.text)
        val valueType = maybePrecision match {
          case Some("Single") => FloatType
          case Some("Double") => DoubleType
          case other => throw new InvalidMetaDataException(csrPath, "resultInfo/prop(name=precision) is invalid")
        }
        val maybeEndianType = resultInfoProperties
          .find(node => (node \ "@name").text == "endianType")
          .map(_ \ "@value")
          .map(_.text)
        val endianType = maybeEndianType match {
          case Some("LittleEndian") => ByteOrdering.LittleEndian
          case Some("BigEndian") => ByteOrdering.BigEndian
          case other => throw new InvalidMetaDataException(csrPath, "resultInfo/prop(name=endianType) is invalid")
        }
        val timePoints = root \ "timeDimensionInfo" \ "timeList" \ "timePoint"
        val times = timePoints.map(t => (t \ "@value").text).toIndexedSeq
        val saDescriptors = root \ "simulatableDimensionInfo" \ "SADescriptor"
        val instruments = saDescriptors.map(i => (i \ "@id").text).toIndexedSeq
        val scenarioInfos = root \ "scenarioDimensionInfo" \ "scenarioList" \ "scenarioInfo"
        val scenarios = scenarioInfos.map(s => (s \ "@name").text).toIndexedSeq
        new MtfCubeRelation(path, times, instruments, scenarios, endianType, valueType, checkCube)(sqlContext)
      }
    }

    tryRelation match {
      case Success(relation) => relation
      case Failure(throwable) => throw throwable
    }
  }

}