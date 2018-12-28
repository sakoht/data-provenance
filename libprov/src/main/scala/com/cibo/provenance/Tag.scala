package com.cibo.provenance

import java.time.Instant
import io.circe._

/**
  * A text string that can be attached to data in a ResultTracker as annotation.
  *
  * @param text
  */
case class Tag(text: String)

object Tag {
  implicit val encoder: Encoder[Tag] = CirceJsonCodec.mkStringEncoder[Tag](_.text)
  implicit val decoder: Decoder[Tag] = CirceJsonCodec.mkStringDecoder[Tag](s => Tag(s))
}

/**
  * This applies a text string Tag to a given call/result.
  *
  * @param cd
  * @tparam T
  */
class ApplyTag[T](implicit cd: Codec[T]) extends Function3WithProvenance[T, Tag, Instant, T] {
  val currentVersion = Version("0.1")
  def impl(obj: T, tag: Tag, timestamp: Instant): T = obj
  override lazy val typeParameterTypeNames: Seq[String] = Seq(Codec.classTagToSerializableName(cd.classTag))
}

object ApplyTag {
  def apply[T : Codec]: ApplyTag[T] = {
    implicit val instantCodec = TagImplicits.instantCodec
    new ApplyTag[T]
  }
}

object TagImplicits {
  implicit val instantEncoder: Encoder[Instant] = CirceJsonCodec.mkStringEncoder(_.toString)
  implicit val instantDecoder: Decoder[Instant] = CirceJsonCodec.mkStringDecoder(Instant.parse)
  implicit val instantCodec: Codec[Instant] = CirceJsonCodec(instantEncoder, instantDecoder)

  /**
    * Extend a ValueWithProvenance[_] to have the addTag(..) method.
    *
    * @param subject  Some call or result.
    * @tparam T       The output type of the subject.
    */
  implicit class Taggable[T : Codec](subject: ValueWithProvenance[T]) {
    /**
      * Add a tag to the subject.
      *
      * @param tag  The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag(tag: ValueWithProvenance[_ <: Tag]): ApplyTag[T]#Call = {
      val applyTagForType = new ApplyTag[T]
      val ts = UnknownProvenance(Instant.now)
      applyTagForType(subject, tag, ts)
    }

    /**
      * Add a tag to the subject by specifying the text String content.
      *
      * @param text The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag(text: String): ApplyTag[T]#Call = {
      val tag = UnknownProvenance(Tag(text))
      addTag(tag)
    }
  }
}
