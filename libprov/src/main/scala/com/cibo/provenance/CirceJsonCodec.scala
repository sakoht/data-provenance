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
@SerialVersionUID(1000L)
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

object CirceJsonCodec {

  /**
    * The simplest encoder, call some function on the object to turn it into a String.
    *
    * Example:
    *   case class Foo(id: String)
    *
    *   object Foo {
    *     implicit val encoder: Encoder[Foo] = CirceJsonCodec.mkStringEncoder[Foo](_.id)
    *   }
    *
    * @param f    Some function which, when applied to T, returns a string.
    * @tparam T   The type to encode
    * @return     A Circe JSON Encoder.
    */
  def mkStringEncoder[T](f: (T) => String): Encoder[T] =
    Encoder.instance {
      (obj: T) =>
        val id = f(obj)
        Encoder.encodeString.apply(id)
    }

  /**
    * The a companion to the simplest encoder, turn a string back into the object.
    *
    * Example:
    *   case class Foo(id: String)
    *
    *   object Foo {
    *     implicit val decoder: Decoder[Foo] = CirceJsonCodec.mkStringDecoder[Foo](Foo.apply)
    *   }
    *
    * @param f    Some function which, when applied to an encoded string, returns a new T.
    * @tparam T   The type to decoder
    * @return     A Circe JSON Decoder.
    */
  def mkStringDecoder[T](f: (String) => T): Decoder[T] =
    Decoder.instance {
      (c: HCursor) =>
        val obj: T = f(c.value.asString.get)
        Right(obj).asInstanceOf[Either[DecodingFailure, T]]
    }
}

