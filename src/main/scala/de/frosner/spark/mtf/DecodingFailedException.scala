package de.frosner.spark.mtf

import scodec.Err

case class DecodingFailedException(cause: Err) extends Exception(cause.toString)
