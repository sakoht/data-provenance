package com.cibo.provenance

import io.circe._
import java.io.Serializable

/**
  * A wrapper around a circe Encoder[T] and Decoder[T].
  *
  * @param encoder  An Encoder[T]
  * @param decoder  A Decoder[T] that matches it.
  * @tparam T       The type of data to be encoded/decoded.
  */
case class Codec[T](encoder: Encoder[T], decoder: Decoder[T]) extends Serializable


object Codec {
  import scala.language.implicitConversions

  /**
    * Implicitly create a Codec[T] wherever an Encoder[T] and Decoder[T] are implicitly available.
    * These can be provided by io.circe.generic.semiauto._ w/o penalty, or .auto._ with some penalty.
    *
    * @tparam T The type of data to be encoded/decoded.
    * @return A Codec[T] created from implicits.
    */
  implicit def createCodec[T : Encoder : Decoder]: Codec[T] =
    Codec(implicitly[Encoder[T]], implicitly[Decoder[T]])

  /**
    * Implicitly create a Codec[ Codec[T] ] so we can serialize/deserialize Codecs.
    * This allows us to fully round-trip data across processes.
    *
    * @tparam T   The type of data underlying the underlying Codec
    * @return     a Codec[ Codec[T] ]
    */
  implicit def selfCodec[T]: Codec[Codec[T]] =
    Codec(new BinaryEncoder[Codec[T]], new BinaryDecoder[Codec[T]])
}


/**
  * This uses the raw Java binary serialization internally, and puts the byte array into
  * simple JSON.  It handles the provenance monads that have arbitrary complex depth,
  * with types known at the base class but arbitrary types in subclasses.
  *
  * @tparam T: Some type (Serializable)
  */
private class BinaryDecoder[T <: Serializable] extends Decoder[T] with Serializable {
  // NOTE: This is its own class, wrapping a regular circe encoder, because the
  // real encoder isn't _itelf_ serializable, and some functions (like map), that
  // take functions inadvertently serialize the encoders themselves.  This makes
  // any case like that work seamlessly, and reconstruct with fidelity later without
  // creating a data payload.

  @transient
  private lazy val dec =  Decoder.forProduct2("bytes", "length")(getObj[T])

  private def getObj[O](bytes: Array[Byte], length: Int): O =
    Util.deserializeRaw(bytes)

  def apply(c: HCursor) = dec.apply(c)
}


/**
  * This uses the raw Java binary serialization internally, and puts the byte array into
  * simple JSON.  It handles the provenance monads that have arbitrary complex depth,
  * with types known at the base class but arbitrary types in subclasses.
  *
  * @tparam T: Some type (serializable)
  */
private class BinaryEncoder[T <: Serializable] extends Encoder[T] with Serializable {
  // NOTE: This is its own class, wrapping a regular circe encoder, because the
  // real encoder isn't _itelf_ serializable, and some functions (like map), that
  // take functions inadvertently serialize the encoders themselves.  This makes
  // any case like that work seamlessly, and reconstruct with fidelity later without
  // creating a data payload.

  @transient
  private lazy val enc: ObjectEncoder[T] =
    Encoder.forProduct2("bytes", "length") {
      obj =>
        val bytes: Array[Byte] =
          try {
            Util.getBytesAndDigestRaw(obj)._1
          } catch {
            case e: Exception =>
              val ee = e
              println(f"Error serializing: $ee")
              throw e
          }
        Tuple2(bytes, bytes.length)
    }

  def apply(a: T): Json = enc.apply(a)
}
