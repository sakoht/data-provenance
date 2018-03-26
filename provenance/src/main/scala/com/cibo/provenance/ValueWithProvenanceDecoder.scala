package com.cibo.provenance

import io.circe._

import scala.util.{Failure, Success, Try}

class ValueWithProvenanceDecoder[O, T <: ValueWithProvenance[O]](implicit rt: ResultTracker) extends Decoder[T] {
  def apply(c: HCursor) = {
    val result: Either[DecodingFailure, ValueWithProvenanceSerializable] = ValueWithProvenanceSerializable.de.apply(c)
    // NOTE: .map is not available in scala 2.11, which is still a target.
    result match {
      case Left(e) =>
        Left(result).asInstanceOf[Either[DecodingFailure, T]]
      case Right(o) =>
        Right(o.load(rt)).asInstanceOf[Either[DecodingFailure, T]]
    }
  }
}

object ValueWithProvenanceDecoder {
  implicit def decoderEncoder[O, T <: ValueWithProvenance[O]]: Encoder[ValueWithProvenanceDecoder[O, T]] =
    new Encoder[ValueWithProvenanceDecoder[O, T]] {
      def apply(a: ValueWithProvenanceDecoder[O, T]) = ???
    }

  implicit def decoderDecoder[O, T <: ValueWithProvenance[O]]: Decoder[ValueWithProvenanceDecoder[O, T]] =
    new Decoder[ValueWithProvenanceDecoder[O, T]] {
      def apply(c: HCursor) = ???
    }

}

/*
class CallDecoder[O, T <: FunctionCallWithProvenance[O]] extends Decoder[T] {
  def apply(c: HCursor): Either[DecodingFailure, T] =  {
    Try {
      val sd: Decoder[FunctionCallWithProvenanceSerializable] = ???
      sd.decodeJson(c.value) match {
        case Right(callSerialized) =>
          val functionName: String = ???
          val function: FunctionWithProvenance[O] = ???
          val call: FunctionCallWithProvenance[O] = function.deserializeCall(callSerialized)
          call
        case Left(error) =>
          throw error
      }
    }.toEither.asInstanceOf[Either[DecodingFailure, T]]
  }
}
*/

