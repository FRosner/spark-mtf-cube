package de.frosner.spark.mtf

import scodec.{Codec, codecs => Codecs}

@SerialVersionUID(1L)
case class SerializableCodec(codecString: String) extends Serializable {

  @transient private lazy val codec: Codec[_] = codecString match {
    case "float" => Codecs.float
    case "floatL"=> Codecs.floatL
    case "double" => Codecs.double
    case "doubleL" => Codecs.doubleL
    case other => throw new IllegalArgumentException(s"Codec '$codecString' invalid")
  }

  def unsafeGet[T]: Codec[T] = codec.asInstanceOf[Codec[T]]

}

object SerializableCodec {

  val Float = SerializableCodec("float")
  val FloatL = SerializableCodec("floatL")

  val Double = SerializableCodec("double")
  val DoubleL = SerializableCodec("doubleL")

}