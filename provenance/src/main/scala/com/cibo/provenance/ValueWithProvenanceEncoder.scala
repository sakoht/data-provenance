package com.cibo.provenance

import io.circe._

class ValueWithProvenanceEncoder[O, T <: ValueWithProvenance[O]](implicit rt: ResultTracker) extends Encoder[T] {
  def apply(obj: T): Json =  {
    val serializable: ValueWithProvenanceSerializable = ValueWithProvenanceSerializable.save[O](obj)
    val json: Json = ValueWithProvenanceSerializable.en.apply(serializable)
    json
  }
}
