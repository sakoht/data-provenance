package com.cibo.provenance

import io.circe._


class ValueWithProvenanceEncoder[O, T <: ValueWithProvenance[O]](implicit rt: ResultTracker) extends Encoder[T] {
  def apply(obj: T): Json =  {
    val serializable: ValueWithProvenanceSerializable = ValueWithProvenanceSerializable.save[O](obj)
    val json: Json = ValueWithProvenanceSerializable.en.apply(serializable)
    json
  }
}


object ValueWithProvenanceEncoder {
  implicit def decoderEncoder[O, T <: ValueWithProvenance[O]]: Encoder[ValueWithProvenanceEncoder[O, T]] =
    new Encoder[ValueWithProvenanceEncoder[O, T]] {
      def apply(a: ValueWithProvenanceEncoder[O, T]) = ???
    }

  implicit def decoderDecoder[O, T <: ValueWithProvenance[O]]: Decoder[ValueWithProvenanceEncoder[O, T]] =
    new Decoder[ValueWithProvenanceEncoder[O, T]] {
      def apply(c: HCursor) = ???
    }

  def create[O, T <: ValueWithProvenance[O]](outputClass: Class[O], monadClass: Class[T])(implicit rt: ResultTracker)
    : ValueWithProvenanceEncoder[O, T] =
      new ValueWithProvenanceEncoder[O, T]
}


/*
class CallEncoder[O, T <: FunctionCallWithProvenance[O]] extends Encoder[T] {
  def apply(call: T): Json =  {
    val functionName = call.functionName
    val serialized = ???
    //val e = FunctionCallWithProvenance.createEncoder[T]
    //val serializable: ValueWithProvenanceSerializable = FunctionCallWithProvenance.(call)
    //val json: Json = ValueWithProvenanceSerializable.en.apply(serializable)
    //json
    ???
  }
}
*/
