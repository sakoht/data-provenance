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
  * @tparam S   The type of data to which the tag will apply.
  */
class AddTag[T : Codec, S : Codec] extends Function3WithProvenance[S, T, Instant, S] {
  val currentVersion = Version("0.1")
  def impl(obj: S, tag: T, timestamp: Instant): S = obj
  override lazy val typeParameterTypeNames: Seq[String] =
    Seq(
      Codec.classTagToSerializableName(implicitly[Codec[T]].classTag),
      Codec.classTagToSerializableName(implicitly[Codec[S]].classTag)
    )
}

object AddTag {
  def apply[T: Codec, S : Codec]: AddTag[T, S] = {
    implicit val instantCodec = TagImplicits.instantCodec
    new AddTag[T, S]
  }
}

/**
  * This function "logically" removes a Tag from a given call/result.
  *
  * Because the storage of calls is append-only, we don't delete things,
  * but this nullifies a previous application.
  *
  * @tparam S   The type of data to which the tag will apply.
  */
class RemoveTag[T : Codec, S : Codec] extends Function3WithProvenance[S, T, Instant, S] {
  val currentVersion = Version("0.1")
  def impl(obj: S, tag: T, timestamp: Instant): S = obj
  override lazy val typeParameterTypeNames: Seq[String] =
    Seq(
      Codec.classTagToSerializableName(implicitly[Codec[T]].classTag),
      Codec.classTagToSerializableName(implicitly[Codec[S]].classTag)
    )
}

object RemoveTag {
  def apply[T : Codec, S : Codec]: RemoveTag[T, S] = {
    implicit val instantCodec = TagImplicits.instantCodec
    new RemoveTag[T, S]
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
  implicit class TaggableResult[S : Codec](subject: Result[S])(implicit rt: ResultTracker) {
    /**
      * Add a tag to the subject.
      *
      * @param tag  The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag[T : Codec](tag: ValueWithProvenance[_ <: T]): AddTag[T, S]#Result = {
      val applyTagForType = new AddTag[T, S]
      val ts = UnknownProvenance(Instant.now)
      applyTagForType(subject, tag, ts).resolve
    }

    /**
      * Add a tag to the subject by specifying the text String content.
      *
      * @param text The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag(text: String): AddTag[Tag, S]#Result = {
      val tag = UnknownProvenance(Tag(text))
      addTag(tag)
    }

    /**
      * Remove a tag from the subject.
      *
      * @param tag  The tag to remove.
      * @return     An RemoveTag[T]#Call that will save the tag when resolved.
      */
    def removeTag[T : Codec](tag: ValueWithProvenance[_ <: T]): RemoveTag[T, S]#Result = {
      val applyTagForType = new RemoveTag[T, S]
      val ts = UnknownProvenance(Instant.now)
      applyTagForType(subject, tag, ts).resolve
    }

    /**
      * Remove a tag from the subject by specifying the text String content.
      *
      * @param text The tag to remove.
      * @return     An RemoveTag[T]#Call that will save the tag when resolved.
      */
    def removeTag(text: String): RemoveTag[Tag, S]#Result = {
      val tag = UnknownProvenance(Tag(text))
      removeTag(tag)
    }    
  }

  implicit class TaggableCall[S : Codec](subject: Call[S]) {
    /**
      * Add a tag to the subject.
      *
      * @param tag  The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag[T : Codec](tag: ValueWithProvenance[_ <: T]): AddTag[T, S]#Call = {
      val applyTagForType = new AddTag[T, S]
      val ts = UnknownProvenance(Instant.now)
      applyTagForType(subject, tag, ts)
    }

    /**
      * Add a tag to the subject by specifying the text String content.
      *
      * @param text The tag to add.
      * @return     An AddTag[T]#Call that will save the tag when resolved.
      */
    def addTag(text: String): AddTag[Tag, S]#Call = {
      val tag = UnknownProvenance(Tag(text))
      addTag(tag)
    }

    /**
      * Remove a tag from the subject.
      *
      * @param tag  The tag to remove.
      * @return     An RemoveTag[T]#Call that will save the tag when resolved.
      */
    def removeTag[T : Codec](tag: ValueWithProvenance[_ <: T]): RemoveTag[T, S]#Call = {
      val applyTagForType = new RemoveTag[T, S]
      val ts = UnknownProvenance(Instant.now)
      applyTagForType(subject, tag, ts)
    }

    /**
      * Remove a tag from the subject by specifying the text String content.
      *
      * @param text The tag to remove.
      * @return     An RemoveTag[T]#Call that will save the tag when resolved.
      */
    def removeTag(text: String): RemoveTag[Tag, S]#Call = {
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
  * @param subjectData
  * @param tag
  * @param ts
  */
case class TagHistoryEntry(addOrRemove: AddOrRemoveTag.Value, subjectData: FunctionCallResultWithProvenanceSerializable, tag: Tag, ts: Instant) {
  def subjectVivified(implicit rt: ResultTracker): FunctionCallResultWithProvenance[_] = subjectData.load(rt)
}

object AddOrRemoveTag extends Enumeration {
  val AddTag = Value("add")
  val RemoveTag = Value("remove")
}

