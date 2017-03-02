package de.frosner.csd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CsdRelation(location: String, userSchema: Option[StructType])
                 (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {

  val sparkContext = sqlContext.sparkContext

  override def schema: StructType = {
    userSchema.getOrElse(
      StructType(Seq(
        StructField("string", StringType, nullable = true)
      ))
    )
  }

  override def buildScan(): RDD[Row] = {
    val byteRecords = sparkContext
      .binaryRecords(location, 2, sparkContext.hadoopConfiguration)
    val strings = byteRecords.map(bytes => new String(bytes.map(_.toChar)))
    val rows = strings.map(string => Row.fromSeq(Seq(string)))
    rows
  }

}