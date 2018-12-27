package com.cibo.provenance

import java.time.Instant

object Util {

  import io.circe._

  implicit val encoder: Encoder[Instant] = Encoder.instance {
    obj: Instant =>
      val id = obj.toString
      Encoder.encodeString.apply(id)
  }

  implicit val decoder: Decoder[Instant] = Decoder.instance {
    c: HCursor =>
      val obj: Instant = Instant.parse(c.value.asString.get)
      Right(obj).asInstanceOf[Either[DecodingFailure, Instant]]
  }

  class tagWithType[T](implicit cd: Codec[T]) extends Function3WithProvenance[T, String, Instant, T] {
    val currentVersion = Version("0.1")
    def impl(obj: T, tagName: String, timestamp: Instant): T = obj
    override lazy val typeParameterTypeNames: Seq[String] = Seq(Codec.classTagToSerializableName(cd.classTag))
  }

  class tag[T](implicit cd: Codec[T]) extends Function3WithProvenance[T, String, Instant, T] {
    val currentVersion = Version("0.1")
    def impl(obj: T, tagName: String, timestamp: Instant): T = obj
    override lazy val typeParameterTypeNames: Seq[String] = Seq(Codec.classTagToSerializableName(cd.classTag))
  }

  object tag {
    def apply[T](
      subject: ValueWithProvenance[_ <: T],
      tagName: ValueWithProvenance[String]
    )(implicit cd: Codec[T]): tag[T]#Call = {
      val tagFunc = new tag[T]
      val tagged = tagFunc(subject, tagName, UnknownProvenance(Instant.now))
      tagged
    }
  }
}
