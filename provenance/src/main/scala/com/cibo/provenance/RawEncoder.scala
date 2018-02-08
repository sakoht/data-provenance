package com.cibo.provenance

/**
  * Apply this trait to the companion object of any class that won't encode nicely with circe.
  *
  * Example:
  *
  *   class MyComplexThing {...}
  *   object MyComplexThing extends RawEncoder[MyComplexThing]
  *
  * @tparam T Some type that doesn't encode nicely automatically with circe,
  *           but will encode well with raw Serializable serialization.
  */
trait RawEncoder[T] {
  import io.circe._
  implicit val decoder: Decoder[T] = Util.rawDecoder[T]
  implicit val encoder: Encoder[T] = Util.rawEncoder[T]
}
