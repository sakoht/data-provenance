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

  def classTag: ClassTag[T] = implicitly[ClassTag[T]]

  def typeTag: TypeTag[T] = implicitly[TypeTag[T]]

  def serialize(obj: T, checkConsistency: Boolean = true): Array[Byte] = {
    implicit val e = this.encoder
    implicit val d = this.decoder

    if (obj.isInstanceOf[ValueWithProvenanceSerializable] && this != ValueWithProvenanceSerializable.codec) {
      println("Odd codec!")
    }

    val json: String = obj.asJson.noSpaces

    if (checkConsistency) {
      //if (json.contains("{"))
      //  println(f"OBJ:$obj\nJSON: $json")

      decode[T](json) match {
        case Right(obj2) =>
          val json2: String = obj2.asJson.noSpaces
          if (json2 != json) {
            throw new RuntimeException(f"Failure to serialize consistently!\n$obj\n$obj2\n$json\n$json2\n$json2")
          }
        case Left(e) =>
          throw e
      }
    }

    json.getBytes("UTF-8")
  }

  def deserialize(bytes: Array[Byte]): T = {
    implicit val e = encoder
    implicit val d = decoder
    val s = new String(bytes, "UTF-8")
    decode[T](s) match {
      case Left(error) =>
        throw error
      case Right(obj) =>
        obj
    }
  }
}

