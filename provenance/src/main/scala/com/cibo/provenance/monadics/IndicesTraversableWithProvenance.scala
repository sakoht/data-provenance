package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/22/17.
  *
  * A builtin FunctionWithProvenance that calls `indices` on a Traversable.
  *
  * NOTE: This implementation maintains the higher-order type S[Int]
  * See IndicesRangeWithProvenance to get a simple Range, like the standard .indices call.
  *
  * TODO: This could actually shortcut and _not_ resolve its inputs before returning indices.
  * We know that myList.map(f1).map(f2).map(f3).indices are the indices of myList w/o calling the functions.
  *
  */

import scala.language.higherKinds
import com.cibo.provenance._
import io.circe.{Decoder, Encoder}

import scala.reflect.ClassTag

class IndicesTraversableWithProvenance[S[_], O : Codec](
  implicit hok: implicits.Traversable[S],
  cdsi: Codec[S[Int]]
) extends Function1WithProvenance[S[O], S[Int]]  {

  val currentVersion: Version = NoVersion

  def impl(s: S[O]): S[Int] = hok.indicesTraversable(s)

  override lazy val typeParameterTypeNames: Seq[String] =
    Seq(hok.outerClassTag, implicitly[Codec[O]].valueClassTag).map(ct => ReflectUtil.classToName(ct))
}

object IndicesTraversableWithProvenance {
  def apply[S[_], A : Codec](implicit hok: implicits.Traversable[S], cdsi: Codec[S[Int]]) =
    new IndicesTraversableWithProvenance[S, A]
}