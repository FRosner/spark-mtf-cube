package de.frosner.csd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CsdRelation(location: String, userSchema: StructType)
                 (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    if (this.userSchema != null) {
      this.userSchema
    } else {
      StructType(Seq(
        StructField("string", StringType, nullable = true)
      ))
    }
  }

  override def buildScan(): RDD[Row] = {
    val rdd = sqlContext
      .sparkContext
      .textFile(location)
    val rows = rdd.map(line => Row.fromSeq(Seq(line)))
    rows
  }

}