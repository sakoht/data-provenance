package com.cibo.provenance

import io.circe._
import java.io.Serializable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag


/**
  * A wrapper around a circe Encoder[T] and Decoder[T], with utility methods used to save/load.
  *
  * @param encoder  An Encoder[T]
  * @param decoder  A Decoder[T] that matches it.
  * @tparam T       The type of data to be encoded/decoded.
  */
case class CirceJsonCodec[T : ClassTag : TypeTag](encoder: Encoder[T], decoder: Decoder[T]) extends Codec[T] with Serializable {
  import io.circe.parser._, io.circe.syntax._

  lazy val classTag: ClassTag[T] = implicitly[ClassTag[T]]

  lazy val typeTag: TypeTag[T] = implicitly[TypeTag[T]]

  def serialize(obj: T): Array[Byte] =
    obj.asJson(encoder).noSpaces.getBytes("UTF-8")

  def deserialize(bytes: Array[Byte]): T =
    decode[T](new String(bytes, "UTF-8"))(decoder) match {
      case Left(error) =>
        throw error
      case Right(obj) =>
        obj
    }
}

