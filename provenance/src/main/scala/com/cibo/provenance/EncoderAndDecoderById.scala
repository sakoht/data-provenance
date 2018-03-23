package com.cibo.provenance

import io.circe.{Decoder, DecodingFailure, Encoder, HCursor}

trait EncoderAndDecoderById[T] {
  def apply(id: String): T

  def unapply(obj: T): String

  implicit val encoder: Encoder[T] = Encoder.instance {
    (obj: T) =>
      val id = unapply(obj)
      Encoder.encodeString.apply(id)
  }

  implicit val decoder: Decoder[T] = Decoder.instance {
    (c: HCursor) =>
      val obj: T = apply(c.value.asString.get)
      Right(obj).asInstanceOf[Either[DecodingFailure, T]]
  }
}