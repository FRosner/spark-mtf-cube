package de.frosner.csd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{BaseRelation, Filter, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class DefaultSource extends RelationProvider with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, None)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    createRelation(sqlContext, parameters, Some(schema))
  }

  private def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: Option[StructType]): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for our data."))
    new CsdRelation(parameters.get("path").get, schema)(sqlContext)
  }

}