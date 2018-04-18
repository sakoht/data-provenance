package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls `indices` on a Traversable.
  *
  * NOTE: This implementation has a Range output.
  * See IndicesTraversableWithProvenance to maintain the higher-order type S[Int]
  *
  * TODO: This could actually shortcut and _not_ resolve its inputs before returning indices.
  * We know that myList.map(f1).map(f2).map(f3).indices are the indices of myList w/o calling the functions.
  *
  */


import scala.language.higherKinds
import com.cibo.provenance._
import io.circe.{Decoder, Encoder}

import scala.reflect.ClassTag


class IndicesRangeWithProvenance[S[_], A : Codec](
  implicit hok: implicits.Traversable[S],
  oc: Codec[Range]
) extends Function1WithProvenance[S[A], Range]  {

  val currentVersion: Version = NoVersion

  def impl(s: S[A]): Range = hok.indicesRange(s)

  override lazy val typeParameterTypeNames: Seq[String] =
    Seq(hok.outerClassTag, implicitly[Codec[A]].valueClassTag).map(ct => ReflectUtil.classToName(ct))
}

object IndicesRangeWithProvenance {

  implicit val rangeDecoder: Decoder[Range] =
    Decoder.forProduct3("start", "end", "step")(Range.apply)

  implicit val rangeEncoder: Encoder[Range] =
    Encoder.forProduct3("start", "end", "step") {
      obj => Tuple3(obj.start, obj.end, obj.step)
    }

  implicit val rangeCodec: Codec[Range] = Codec(rangeEncoder, rangeDecoder)

  def apply[S[_], A : Codec](implicit converter: implicits.Traversable[S]) =
    new IndicesRangeWithProvenance[S, A]
}

object IndicesOfRangeWithProvenance extends Function1WithProvenance[Range, Range]()(
  IndicesRangeWithProvenance.rangeCodec
) {
  val currentVersion: Version = NoVersion
  def impl(range: Range): Range = range.indices
}
