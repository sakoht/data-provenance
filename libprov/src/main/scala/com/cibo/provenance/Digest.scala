package com.cibo.provenance

/**
  * Created by ssmith on 10/9/17.
  *
  * This is a wrapper for the hex string of the SHA1 of a VirtualOutput.
  * The digest is used as identity and a storage key.
  *
  * @param id: A stringified SHA1 hex value.
  */
case class Digest(id: String)


object Digest {
  import io.circe._

  implicit val encoder: Encoder[Digest] = Encoder.instance {
    (obj: Digest) =>
      val id = unapply(obj).get
      Encoder.encodeString.apply(id)
  }

  implicit val decoder: Decoder[Digest] = Decoder.instance {
    (c: HCursor) =>
      val obj: Digest = apply(c.value.asString.get)
      Right(obj).asInstanceOf[Either[DecodingFailure, Digest]]
  }

  implicit val codec: Codec[Digest] = Codec(encoder, decoder)
}