package com.cibo.provenance

import io.circe._

/**
  * This encoder is ResultTracker-specific.  It is fabricated during the save process by the tracker.
  *
  * @param rt The ResultTracker that "owns" the save process.
  * @tparam O The output type of the ValueWithProvenance.
  * @tparam T The overall type of ValueWithProvenance being saved.
  */
class ValueWithProvenanceEncoder[O, T <: ValueWithProvenance[O]](
  @transient
  implicit val rt: ResultTracker
) extends Encoder[T] {
  def apply(obj: T): Json =  {
    val serializable: ValueWithProvenanceSerializable = ValueWithProvenanceSerializable.save[O](obj)
    val json: Json = ValueWithProvenanceSerializable.codec.encoder.apply(serializable)
    json
  }
}
