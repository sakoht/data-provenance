package com.cibo.provenance

/**
  * Created by ssmith on 10/6/17.
  *
  * This encapsulates a value such that it can go from a real object to a serialized blob to a digest and back.
  *
  */

import com.cibo.provenance.tracker.ResultTracker

import scala.language.implicitConversions
import scala.reflect.ClassTag


case class Deflatable[T](
  valueOption: Option[T],
  digestOption: Option[Digest],
  serializedDataOption: Option[Array[Byte]]
)(implicit ct: ClassTag[T]) {

  lazy val noDataException = new RuntimeException(f"No value, no serialization, and no digest??? $this")

  if (valueOption.isEmpty && digestOption.isEmpty && serializedDataOption.isEmpty)
    throw noDataException

  def className: String = ct.runtimeClass.getName

  def resolveValue(implicit rt: ResultTracker): Deflatable[T] =
    valueOption match {
      case Some(_) =>
        this
      case None =>
        val value: T = serializedDataOption match {
          case Some(blob) =>
            Util.deserialize[T](blob)
          case None => digestOption match {
            case Some(digest) =>
              rt.loadValueOption[T](digest) match {
                case Some(obj) => obj
                case None => throw new RuntimeException(f"Failed to load object for digest $digest")
              }
            case None =>
              throw noDataException
          }
        }
        copy(valueOption = Some(value))
    }

  def resolveSerialization(implicit rt: ResultTracker): Deflatable[T] =
    serializedDataOption match {
      case Some(_) =>
        this
      case None =>
        val serialization = valueOption match {
          case Some(value) =>
            Util.serialize(value)
          case None =>
            digestOption match {
              case Some(digest) =>
                rt.loadValueSerializedDataOption(digest) match {
                  case Some(s) => s
                  case None => throw new RuntimeException(f"Failed to load serialized data for digest $digest")
                }
              case None =>
                throw noDataException
            }
        }
        copy(serializedDataOption = Some(serialization))
    }

  def resolveDigest: Deflatable[T] = digestOption match {
    case Some(_) =>
      this
    case None =>
      val bytes = serializedDataOption match {
        case Some(b) =>
          b
        case None =>
          valueOption match {
            case Some(value) =>
              Util.serialize(value)
            case None =>
              throw noDataException
          }
      }
      val digest = Util.digestBytes(bytes)
      copy(digestOption = Some(digest)) // do not save the serialization as it is heavey and can be remade on the fly
  }
}

object Deflatable {
  def apply[T](obj: T)(implicit ct: ClassTag[T]): Deflatable[T] =
    Deflatable(valueOption = Some(obj), digestOption = None, serializedDataOption = None)

  def unapply[T : ClassTag](v: Deflatable[T])(implicit rt: ResultTracker): T =
    v.resolveValue.valueOption.get

  implicit def toDeflatable[T : ClassTag](obj: T): Deflatable[T] =
    apply(obj)

  implicit def fromDeflatable[T : ClassTag](v: Deflatable[T])(implicit rt: ResultTracker): T =
    unapply(v)

}


