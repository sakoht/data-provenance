package com.cibo.provenance

import io.circe._

import scala.util.{Failure, Success, Try}

class ValueWithProvenanceDecoder[O, T <: ValueWithProvenance[O]](implicit rt: ResultTracker) extends Decoder[T] {
  def apply(c: HCursor) = {
    val result: Either[DecodingFailure, ValueWithProvenanceSerializable] = ValueWithProvenanceSerializable.decoder.apply(c)
    // NOTE: .map is not available in scala 2.11, which is still a target.
    result match {
      case Left(e) =>
        Left(result).asInstanceOf[Either[DecodingFailure, T]]
      case Right(o) =>
        Right(o.load(rt)).asInstanceOf[Either[DecodingFailure, T]]
    }
  }
}
