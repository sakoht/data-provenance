package com.cibo.provenance

import io.circe._
import io.circe.generic.auto._

import scala.reflect.ClassTag

/**
  * ValueWithProvenanceSerializable is a companion to the ValueWithProvenance sealed trait hierarchy.
  *
  * Each class in the tree is paired with another in the ValueWithProvenance tree,
  * but is structured to be serialized:
  * - type information is translated into values
  * - nested values are serialized recursively, then referenced by storage ID rather than software reference
  * - storage IDs come from a digest of the consistently serialized data
  *
  * Each case class serializes as JSON.
  *
  * The companion object has an save() method that takes the related class and makes the *Saved class,
  * along with an implicit ResultTracker, and actually "saves".
  *
  * A matching load() method does the converse and turns a saved stub into a full object.
  *
  */
sealed trait ValueWithProvenanceSerializable {
  def load(implicit rt: ResultTracker): ValueWithProvenance[_] =
    ValueWithProvenanceSerializable.load(this)(rt)

  def wrap: ValueWithProvenanceSaved[_]
}


object ValueWithProvenanceSerializable {
  import io.circe.generic.semiauto._

  implicit val en: Encoder[ValueWithProvenanceSerializable] = deriveEncoder[ValueWithProvenanceSerializable]
  implicit val de: Decoder[ValueWithProvenanceSerializable] = deriveDecoder[ValueWithProvenanceSerializable]

  def save[O](value: ValueWithProvenance[O])(implicit rt: ResultTracker): ValueWithProvenanceSerializable = {
    value match {
      case result: FunctionCallResultWithProvenance[O] =>
        FunctionCallResultWithKnownProvenanceSerializable.save(result)(rt)
      case call: FunctionCallWithProvenance[O] =>
        FunctionCallWithProvenanceSerializable.save(call)(rt)
      case other =>
        throw new RuntimeException(f"Cannot untypify $other to a ValueWithProvenanceSaved! (Implement me.)")
    }
  }

  def load(saved: ValueWithProvenanceSerializable)(implicit rt: ResultTracker): ValueWithProvenance[_] = {
    saved match {
      case result: FunctionCallResultWithKnownProvenanceSerializable =>
        FunctionCallResultWithKnownProvenanceSerializable.load(result)
      case call: FunctionCallWithProvenanceSerializable =>
        FunctionCallWithProvenanceSerializable.load(call)
      case other =>
        throw new RuntimeException(f"Unexpected type $other into a ValueWithProvenance!")
    }
  }
}


sealed trait FunctionCallWithProvenanceSerializable extends ValueWithProvenanceSerializable {
  def outputClassName: String
  override def load(implicit rt: ResultTracker): FunctionCallWithProvenance[_] =
    FunctionCallWithProvenanceSerializable.load(this)(rt)

  def wrap: FunctionCallWithProvenanceSaved[_] = FunctionCallWithProvenanceSaved(this)
}


object FunctionCallWithProvenanceSerializable {
  def save[O](call: FunctionCallWithProvenance[O])(implicit rt: ResultTracker): FunctionCallWithProvenanceSerializable = {
    call match {
      case u: UnknownProvenance[O] =>
        FunctionCallWithUnknownProvenanceSerializable.save[O](u)(rt)
      case known: FunctionCallWithProvenance[O] =>
        FunctionCallWithKnownProvenanceSerializable.save[O](known)(rt)
    }
  }

  def load(saved: FunctionCallWithProvenanceSerializable)(implicit rt: ResultTracker): FunctionCallWithProvenance[_] = {
    saved match {
      case unknown: FunctionCallWithUnknownProvenanceSerializable =>
        FunctionCallWithUnknownProvenanceSerializable.loadWithUnknownType(unknown)
      case noI: FunctionCallWithKnownProvenanceSerializableWithoutInputs =>
        FunctionCallWithKnownProvenanceSerializableWithoutInputs.load(noI)
      case withI: FunctionCallWithKnownProvenanceSerializableWithInputs =>
        FunctionCallWithKnownProvenanceSerializableWithInputs.load(withI)
    }
  }
}


case class FunctionCallWithUnknownProvenanceSerializable(
  outputClassName: String,
  valueDigest: Digest
) extends FunctionCallWithProvenanceSerializable


object FunctionCallWithUnknownProvenanceSerializable {
  def save[O](call: UnknownProvenance[O])(implicit rt: ResultTracker): FunctionCallWithUnknownProvenanceSerializable = {
    implicit val outputClassTag: ClassTag[O] = call.outputClassTag
    implicit val outputEncoder: io.circe.Encoder[O] = call.outputEncoder
    implicit val outputDecoder: io.circe.Decoder[O] = call.outputDecoder

    val outputClassName = Util.classToName(outputClassTag)

    val rts = rt.asInstanceOf[ResultTrackerSimple]
    val valueDigest: Digest = rts.saveOutputValue(call.value)

    FunctionCallWithUnknownProvenanceSerializable(
      outputClassName,
      valueDigest
    )
  }

  def loadWithUnknownType(saved: FunctionCallWithUnknownProvenanceSerializable)(implicit rt: ResultTracker): UnknownProvenance[_] = {
    val clazz: Class[_] = Class.forName(saved.outputClassName)
    loadWithUnknownEncoderDecoder(saved, clazz)
  }

  def loadWithUnknownEncoderDecoder[T](saved: FunctionCallWithUnknownProvenanceSerializable, clazz: Class[T])(implicit rt: ResultTracker): UnknownProvenance[_] = {
    implicit val ct: ClassTag[T] = ClassTag(clazz)
    val rts = rt.asInstanceOf[ResultTrackerSimple]
    val (value, codec) = rts.loadValueWithCodec[T](saved.valueDigest)
    implicit val en: Encoder[T] = codec.encoder
    implicit val de: Decoder[T] = codec.decoder
    UnknownProvenance(value)
  }

  def load[T : ClassTag : Encoder : Decoder](saved: FunctionCallWithUnknownProvenanceSerializable)(implicit rt: ResultTracker): UnknownProvenance[_] = {
    val value: T = rt.loadValue[T](saved.valueDigest)
    UnknownProvenance(value)
  }
}


sealed trait FunctionCallWithKnownProvenanceSerializable extends FunctionCallWithProvenanceSerializable {
  def functionName: String
  def functionVersion: Version
  def outputClassName: String
}


object FunctionCallWithKnownProvenanceSerializable {
  def save[O](call: FunctionCallWithProvenance[O])(implicit rt: ResultTracker): FunctionCallWithKnownProvenanceSerializableWithInputs =
    FunctionCallWithKnownProvenanceSerializableWithInputs.save(call)

  def load(saved: FunctionCallWithKnownProvenanceSerializableWithInputs)(implicit rt: ResultTracker): FunctionCallWithProvenance[_] =
    FunctionCallWithKnownProvenanceSerializableWithInputs.load(saved)

  def load(saved: FunctionCallWithKnownProvenanceSerializableWithoutInputs)(implicit rt: ResultTracker): FunctionCallWithProvenance[_] =
    FunctionCallWithKnownProvenanceSerializableWithoutInputs.load(saved)
}


case class FunctionCallWithKnownProvenanceSerializableWithInputs(
  functionName: String,
  functionVersion: Version,
  outputClassName: String,
  inputList: List[ValueWithProvenanceSerializable]
) extends FunctionCallWithKnownProvenanceSerializable {

  @transient
  private lazy val bytesAndDigest: (Array[Byte], Digest) = Util.getBytesAndDigest(this)
  def toBytes: Array[Byte] = bytesAndDigest._1
  def toDigest: Digest = bytesAndDigest._2

  @transient
  lazy val inputGroupBytesAndDigest: (Array[Byte], Digest) = Util.getBytesAndDigest(inputValueDigests.map(_.id))
  def inputGroupBytes: Array[Byte] = inputGroupBytesAndDigest._1
  def inputGroupDigest: Digest = inputGroupBytesAndDigest._2

  def unexpandInputs: FunctionCallWithKnownProvenanceSerializableWithoutInputs =
    FunctionCallWithKnownProvenanceSerializableWithoutInputs(
      functionName,
      functionVersion,
      outputClassName,
      toDigest
    )

  def inputResultVector: Vector[FunctionCallResultWithProvenanceSerializable] =
    inputList.map(_.asInstanceOf[FunctionCallResultWithProvenanceSerializable]).toVector

  def inputValueDigests: Vector[Digest] =
    inputResultVector.map(_.outputDigest)
}


object FunctionCallWithKnownProvenanceSerializableWithInputs {
  import io.circe.generic.auto._

  def save[O](call: FunctionCallWithProvenance[O])(implicit rt: ResultTracker): FunctionCallWithKnownProvenanceSerializableWithInputs = {
    implicit val outputClassTag: ClassTag[O] = call.outputClassTag
    implicit val outputEncoder: io.circe.Encoder[O] = call.outputEncoder
    implicit val outputDecoder: io.circe.Decoder[O] = call.outputDecoder

    val outputClassName = Util.classToName(outputClassTag)

    val callInSavableForm =
      call.versionValueAlreadyResolved match {
        case Some(versionValue) =>
          FunctionCallWithKnownProvenanceSerializableWithInputs(
            call.functionName,
            versionValue,
            outputClassName,
            call.inputs.toList.map {
              case dcall: FunctionCallWithProvenanceSaved[_] =>
                dcall.data
              case dresult: FunctionCallResultWithProvenanceSaved[_] =>
                dresult.data
              case u: UnknownProvenance[_] =>
                FunctionCallWithUnknownProvenanceSerializable.save(u)(rt)
              case u: UnknownProvenanceValue[_] =>
                FunctionCallResultWithUnknownProvenanceSerializable.save(u)(rt)
              case kcall: FunctionCallWithProvenance[_] =>
                FunctionCallWithKnownProvenanceSerializableWithInputs.save(kcall)(rt).unexpandInputs
              case kresult: FunctionCallResultWithProvenance[_] =>
                FunctionCallResultWithKnownProvenanceSerializable.save(kresult)(rt)
              case other =>
                throw new RuntimeException(f"Unexpected sub-type of ValueWithProvenance: $other")
            }
          )
        case None =>
          // While this call cannot be saved directly, any downstream call will be allowed to wrap it.
          // (This exception is intercepted in the block above of the downstream call.)
          throw new RuntimeException("UnresolvedVersionException(known)")
      }

    rt.saveCall(callInSavableForm)

    callInSavableForm
  }

  def load(
    saved: FunctionCallWithKnownProvenanceSerializableWithInputs
  )(implicit rt: ResultTracker): FunctionCallWithProvenance[_] = {
    val clazz = Class.forName(saved.outputClassName)
    load(saved, clazz)(rt)
  }

  private def load[T](
    saved: FunctionCallWithKnownProvenanceSerializableWithInputs,
    clazz: Class[T]
  )(implicit rt: ResultTracker): FunctionCallWithProvenance[T] = {
    val f = FunctionWithProvenance.getByName(saved.functionName)
    val call: FunctionCallWithProvenance[_] = f.deserializeCall(saved)
    call.asInstanceOf[FunctionCallWithProvenance[T]]
  }
}


case class FunctionCallWithKnownProvenanceSerializableWithoutInputs(
  functionName: String,
  functionVersion: Version,
  outputClassName: String,
  digestOfEquivalentWithInputs: Digest
) extends FunctionCallWithKnownProvenanceSerializable {
  def expandInputs(rts: ResultTracker): FunctionCallWithKnownProvenanceSerializableWithInputs =
    rts.loadCallMetaByDigest(functionName, functionVersion, digestOfEquivalentWithInputs) match {
      case Some(call) =>
        call
      case None =>
        throw new RuntimeException("")
    }
}


object FunctionCallWithKnownProvenanceSerializableWithoutInputs {

  def save[O](call: FunctionCallWithProvenance[O])(implicit rt: ResultTracker): FunctionCallWithKnownProvenanceSerializableWithoutInputs =
    FunctionCallWithKnownProvenanceSerializableWithInputs.save(call).unexpandInputs

  def load(saved: FunctionCallWithKnownProvenanceSerializableWithoutInputs)(implicit rt: ResultTracker): FunctionCallWithProvenance[_] =
    FunctionCallWithKnownProvenanceSerializableWithInputs.load(saved.expandInputs(rt))
}


sealed trait FunctionCallResultWithProvenanceSerializable extends ValueWithProvenanceSerializable {
  def call: FunctionCallWithProvenanceSerializable
  def inputGroupDigest: Digest
  def outputDigest: Digest
  def commitId: String
  def buildId: String

  def wrap: FunctionCallResultWithProvenanceSaved[_] = FunctionCallResultWithProvenanceSaved(this)
}

object FunctionCallResultWithProvenanceSerializable {

  def save[O](result: FunctionCallResultWithProvenance[O])(implicit rt: ResultTracker): FunctionCallResultWithProvenanceSerializable = {
    result.call match {
      case _: UnknownProvenance[O] => FunctionCallResultWithUnknownProvenanceSerializable.save(result)
      case _ => FunctionCallResultWithKnownProvenanceSerializable.save(result)
    }
  }
}


case class FunctionCallResultWithKnownProvenanceSerializable(
  call: FunctionCallWithKnownProvenanceSerializableWithoutInputs,
  inputGroupDigest: Digest,
  outputDigest: Digest,
  commitId: String,
  buildId: String
) extends FunctionCallResultWithProvenanceSerializable


object FunctionCallResultWithKnownProvenanceSerializable {
  def save[O](result: FunctionCallResultWithProvenance[O])(implicit rt: ResultTracker): FunctionCallResultWithKnownProvenanceSerializable = {
    val call = result.call

    rt.saveBuildInfo

    val callSavedWithInputs: FunctionCallWithKnownProvenanceSerializableWithInputs =
      FunctionCallWithProvenanceSerializable.save(call)(rt).asInstanceOf[FunctionCallWithKnownProvenanceSerializableWithInputs]

    val output = result.output
    implicit val outputClassTag: ClassTag[O] = call.outputClassTag
    implicit val outputEncoder: io.circe.Encoder[O] = call.outputEncoder
    implicit val outputDecoder: io.circe.Decoder[O] = call.outputDecoder
    val outputDigest = rt.saveOutputValue(output) //result.resolveAndExtractDigest

    val resultInSavableForm =
      FunctionCallResultWithKnownProvenanceSerializable(
        callSavedWithInputs.unexpandInputs,
        callSavedWithInputs.inputGroupDigest,
        Util.digestObject(result.output(rt)),
        result.outputBuildInfo.commitId,
        result.outputBuildInfo.buildId
      )

    val in: Vector[FunctionCallResultWithProvenanceSerializable] = callSavedWithInputs.inputResultVector
    rt.saveResult(resultInSavableForm, in)

    resultInSavableForm
  }

  def load(saved: FunctionCallResultWithKnownProvenanceSerializable)(implicit rt: ResultTracker): FunctionCallResultWithProvenance[_] = {
    val clazz = Class.forName(saved.call.outputClassName)
    load(saved, clazz)
  }

  private def load[T](saved: FunctionCallResultWithKnownProvenanceSerializable, clazz: Class[T])(implicit rt: ResultTracker): FunctionCallResultWithProvenance[T] = {
    val call = FunctionCallWithProvenanceSerializable.load(saved.call)(rt).asInstanceOf[FunctionCallWithProvenance[T]]
    implicit val en = call.outputEncoder
    implicit val de = call.outputDecoder
    implicit val ct = call.outputClassTag
    val bi = BuildInfoBrief(saved.commitId, saved.buildId)
    val output: T = rt.loadValue[T](saved.outputDigest)
    call.newResult(VirtualValue(output)(ct))(bi)
  }
}


case class FunctionCallResultWithUnknownProvenanceSerializable(
  call: FunctionCallWithUnknownProvenanceSerializable,
  outputDigest: Digest,
  commitId: String,
  buildId: String
) extends FunctionCallResultWithProvenanceSerializable {
  val inputGroupDigest: Digest = Util.digestObject(List[Digest]())
}


object FunctionCallResultWithUnknownProvenanceSerializable {
  def save[O](result: FunctionCallResultWithProvenance[O])(implicit rt: ResultTracker): FunctionCallResultWithUnknownProvenanceSerializable = {
    val call = result.call

    rt.saveBuildInfo

    val callSaved: FunctionCallWithUnknownProvenanceSerializable =
      FunctionCallWithProvenanceSerializable.save(call)(rt).asInstanceOf[FunctionCallWithUnknownProvenanceSerializable]

    val resultInSavableForm =
      FunctionCallResultWithUnknownProvenanceSerializable(
        callSaved,
        callSaved.valueDigest,
        result.outputBuildInfo.commitId,
        result.outputBuildInfo.buildId
      )

    resultInSavableForm
  }
}

