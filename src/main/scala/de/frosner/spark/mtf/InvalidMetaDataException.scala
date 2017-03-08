package de.frosner.spark.mtf

case class InvalidMetaDataException(file: String, reason: String)
  extends Exception(s"Could not parse meta data file $file: $reason")
