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
  implicit val encoder: Encoder[Digest] = CirceJsonCodec.mkStringEncoder[Digest](_.id)
  implicit val decoder: Decoder[Digest] = CirceJsonCodec.mkStringDecoder(Digest.apply)
  implicit val codec: Codec[Digest] = Codec(encoder, decoder)
}