package com.cibo.provenance

import io.circe.{Decoder, Encoder}

case class Codec[T](encoder: Encoder[T], decoder: Decoder[T]) extends Serializable

object Codec {
  import scala.language.implicitConversions

  def fromImplicits[T : Encoder : Decoder]: Codec[T] =
    Codec(implicitly[Encoder[T]], implicitly[Decoder[T]])

  // Create these implicitly where an encoder/decoder pair is available.
  // This just cuts down on the number of things passed-around.
  implicit def mk[T : Encoder : Decoder]: Codec[T] = Codec.fromImplicits[T]

  // Ando convert back to either an encoder or decoder where needed.
  implicit def getDecoder[T](codec: Codec[T]): Decoder[T] = codec.decoder
  implicit def getEncoder[T](codec: Codec[T]): Encoder[T] = codec.encoder

  // Make a Codec for an Option[T] wherever there is a Codec for the underlying T
  implicit def mkOpt[T : Codec]: Codec[Option[T]] = {
    val c: Codec[T] = implicitly[Codec[T]]
    implicit val e: Encoder[T] = c.encoder
    implicit val d: Decoder[T] = c.decoder
    import io.circe.generic.semiauto._
    implicit val e2: Encoder[Option[T]] = deriveEncoder[Option[T]]
    implicit val d2: Decoder[Option[T]] = deriveDecoder[Option[T]]
    Codec.fromImplicits[Option[T]]
  }

  // Allow encoders and decoders to, ultimately, save and load themselves.
  // These do _not_ need to save with consistent hashes, and are adjacent to the data hierarchy.
  implicit def selfCodec[T]: Codec[Codec[T]] = Codec(selfEncoder[T], selfDecoder[T])
  implicit def selfEncoder[T]: Encoder[Codec[T]] = new BinaryEncoder[Codec[T]]
  implicit def selfDecoder[T]: Decoder[Codec[T]] = new BinaryDecoder[Codec[T]]
}
