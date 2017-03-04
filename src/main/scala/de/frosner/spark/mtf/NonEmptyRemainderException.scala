package de.frosner.spark.mtf

import scodec.bits.BitVector

case class NonEmptyRemainderException(remainder: BitVector)
  extends Exception(s"Expected empty remainder but got $remainder. " +
    "Either the file is corrupt or the record width is incorrect.")
