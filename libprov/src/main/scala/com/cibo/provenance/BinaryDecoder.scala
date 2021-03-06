package com.cibo.provenance

import java.io.Serializable

import io.circe.{Decoder, HCursor}

/**
  * This uses the raw Java binary serialization internally, and puts the byte array into
  * simple JSON.  It handles the provenance monads that have arbitrary complex depth,
  * with types known at the base class but arbitrary types in subclasses.
  *
  * @tparam T: Some type (Serializable)
  */
class BinaryDecoder[T <: Serializable] extends Decoder[T] with Serializable {
  // NOTE: This is its own class, wrapping a regular circe encoder, because the
  // real encoder isn't _itelf_ serializable, and some functions (like map), that
  // take functions inadvertently serialize the encoders themselves.  This makes
  // any case like that work seamlessly, and reconstruct with fidelity later without
  // creating a data payload.

  @transient
  private lazy val dec =  Decoder.forProduct2("bytes", "length")(getObj[T])

  private def getObj[O](bytes: Array[Byte], length: Int): O =
    Codec.deserializeRaw(bytes)

  def apply(c: HCursor) = dec.apply(c)
}
