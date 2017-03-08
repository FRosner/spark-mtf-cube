package de.frosner.spark.mtf

case class InvalidCubeSizeException(actual: Long, expected: Long)
  extends Exception(s"Expected $expected entries in the cube but got $actual")
