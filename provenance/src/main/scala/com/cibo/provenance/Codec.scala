package com.cibo.provenance

import io.circe._
import java.io.Serializable

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * A wrapper around a circe Encoder[T] and Decoder[T].
  *
  * @param encoder  An Encoder[T]
  * @param decoder  A Decoder[T] that matches it.
  * @tparam T       The type of data to be encoded/decoded.
  */
case class Codec[T : ClassTag : TypeTag](encoder: Encoder[T], decoder: Decoder[T]) extends Serializable {
  def valueClassTag = implicitly[ClassTag[T]]
  def valueTypeTag = implicitly[TypeTag[T]]
  def fullClassName = ReflectUtil.classToName(valueClassTag)
}

object Codec {
  import scala.language.implicitConversions

  /**
    * Implicitly create a Codec[T] wherever an Encoder[T] and Decoder[T] are implicitly available.
    * These can be provided by io.circe.generic.semiauto._ w/o penalty, or .auto._ with some penalty.
    *
    * @tparam T The type of data to be encoded/decoded.
    * @return A Codec[T] created from implicits.
    */
  implicit def createCodec[T : Encoder : Decoder : ClassTag : TypeTag]: Codec[T] =
    Codec(implicitly[Encoder[T]], implicitly[Decoder[T]])

  /**
    * Implicitly create a Codec[ Codec[T] ] so we can serialize/deserialize Codecs.
    * This allows us to fully round-trip data across processes.
    *
    * @tparam T   The type of data underlying the underlying Codec
    * @return     a Codec[ Codec[T] ]
    */
  implicit def selfCodec[T : ClassTag](implicit tt: TypeTag[Codec[T]], ct: ClassTag[Codec[T]]): Codec[Codec[T]] =
    Codec(new BinaryEncoder[Codec[T]], new BinaryDecoder[Codec[T]])

  implicit def toTypeTag[T](codec: Codec[T]): TypeTag[T] = codec.valueTypeTag

  implicit def toClassTag[T](codec: Codec[T]): ClassTag[T] = codec.valueClassTag
}
