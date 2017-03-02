package de.frosner.csd

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class DefaultSourceSpec extends FlatSpec with Matchers {

  "The DefaultSource" should "infer the schema correctly" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    try {
      val df = spark.read.format("de.frosner.csd").load("src/test/resources/example.csd")
      df.show
    } finally {
      spark.stop
    }
  }

}
