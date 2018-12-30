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
  * This function applies a text string Tag to a given call/result.
  * 
  * @param cd   The codec for the type of data to which the function will apply.
  * @tparam S   The type of data to which the tag will apply.
  */
class AddTag[S](implicit cd: Codec[S]) extends Function3WithProvenance[S, Tag, Instant, S] {
  val currentVersion = Version("0.1")
  def impl(obj: S, tag: Tag, timestamp: Instant): S = obj
  override lazy val typeParameterTypeNames: Seq[String] = Seq(Codec.classTagToSerializableName(cd.classTag))
}

object AddTag {
  def apply[S : Codec]: AddTag[S] = {
    implicit val instantCodec = TagImplicits.instantCodec
    new AddTag[S]
  }
}

/**
  * This function "logically" removes a Tag from a given call/result.
  *
  * Because the storage of calls is append-only, we don't delete things,
  * but this nullifies a previous application.
  *
  * @param cd   The codec for the type of data to which the function will apply.
  * @tparam S   The type of data to which the tag will apply.
  */
class RemoveTag[S](implicit cd: Codec[S]) extends Function3WithProvenance[S, Tag, Instant, S] {
  val currentVersion = Version("0.1")
  def impl(obj: S, tag: Tag, timestamp: Instant): S = obj
  override lazy val typeParameterTypeNames: Seq[String] = Seq(Codec.classTagToSerializableName(cd.classTag))
}

object RemoveTag {
  def apply[S : Codec]: RemoveTag[S] = {
    implicit val instantCodec = TagImplicits.instantCodec
    new RemoveTag[S]
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
    * @tparam S       The output type of the subject.
    */
  implicit class Taggable[S : Codec](subject: ValueWithProvenance[S]) {
    /**
      * Add a tag to the subject.
      *
      * @param tag  The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag(tag: ValueWithProvenance[_ <: Tag]): AddTag[S]#Call = {
      val applyTagForType = new AddTag[S]
      val ts = UnknownProvenance(Instant.now)
      applyTagForType(subject, tag, ts)
    }

    /**
      * Add a tag to the subject by specifying the text String content.
      *
      * @param text The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag(text: String): AddTag[S]#Call = {
      val tag = UnknownProvenance(Tag(text))
      addTag(tag)
    }

    /**
      * Remove a tag from the subject.
      *
      * @param tag  The tag to remove.
      * @return     An RemoveTag[T]#Call that will save the tag when resolved.
      */
    def removeTag(tag: ValueWithProvenance[_ <: Tag]): AddTag[S]#Call = {
      val applyTagForType = new AddTag[S]
      val ts = UnknownProvenance(Instant.now)
      applyTagForType(subject, tag, ts)
    }

    /**
      * Remove a tag from the subject by specifying the text String content.
      *
      * @param text The tag to remove.
      * @return     An RemoveTag[T]#Call that will save the tag when resolved.
      */
    def removeTag(text: String): AddTag[S]#Call = {
      val tag = UnknownProvenance(Tag(text))
      removeTag(tag)
    }    
  }
}

/**
  * A TagHistoryEntry is represents either a call to AddTag or RemoveTag in the history of some tag/subject pair.
  *
  * This is a software translation of the call and its inputs, used by the ResultTracker to express history with a clear interface.
  *
  * @param addOrRemove
  * @param subject
  * @param tag
  * @param ts
  */
case class TagHistoryEntry(addOrRemove: AddOrRemoveTag.Value, subject: FunctionCallResultWithProvenanceSerializable, tag: Tag, ts: Instant)

object AddOrRemoveTag extends Enumeration {
  val AddTag = Value("add")
  val RemoveTag = Value("remove")
}

