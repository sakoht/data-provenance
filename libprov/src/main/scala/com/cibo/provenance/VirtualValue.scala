package com.cibo.provenance

import io.circe._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by ssmith on 10/6/17.
  *
  * This encapsulates a value such that it can go from a real object, to a byte array, to a digest and back.
  *
  * @param valueOption            The actual value of type T (optional).
  * @param digestOption           The digest of the serialization of the value T (optional)
  * @param serializedDataOption   The bytes of the serialized value T (optional).
  * @param ct                     The implicit ClassTag[T]
  * @tparam T                     The type of the value eventually returnable.
  */
case class VirtualValue[T](
  valueOption: Option[T],
  digestOption: Option[Digest],
  serializedDataOption: Option[Array[Byte]]
)(implicit ct: Codec[T]) {

  lazy val noDataException = new RuntimeException(f"No value, no serialization, and no digest??? $this")

  if (valueOption.isEmpty && digestOption.isEmpty && serializedDataOption.isEmpty)
    throw noDataException

  def className: String = ct.runtimeClass.getName

  def resolveValue(implicit rt: ResultTracker): VirtualValue[T] =
    valueOption match {
      case Some(_) =>
        this
      case None =>
        val value: T = serializedDataOption match {
          case Some(blob) =>
            Codec.deserialize[T](blob)
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

  def resolveSerialization(implicit rt: ResultTracker): VirtualValue[T] =
    serializedDataOption match {
      case Some(_) =>
        this
      case None =>
        val serialization = valueOption match {
          case Some(value) =>
            Codec.serialize(value)
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

  def resolveDigest: VirtualValue[T] = digestOption match {
    case Some(_) =>
      this
    case None =>
      val bytes = serializedDataOption match {
        case Some(b) =>
          b
        case None =>
          valueOption match {
            case Some(value) =>
              Codec.serialize(value)
            case None =>
              throw noDataException
          }
      }
      val digest = Codec.digestBytes(bytes)
      copy(digestOption = Some(digest)) // do not save the serialization as it is heavey and can be remade on the fly
  }

  override def toString: String = {
    lazy val valueString = valueOption.get.toString
    if (valueOption.nonEmpty & valueString.length < 30) {
      valueString
    } else {
      digestOption match {
        case Some(digest) =>
          "#" + digest.id.toString.substring(0, 5) + " " + super.toString
        case None =>
          super.toString
      }
    }
  }
}

object VirtualValue {
  def apply[T : Codec](obj: T): VirtualValue[T] =
    VirtualValue(valueOption = Some(obj), digestOption = None, serializedDataOption = None)

  def unapply[T : Codec](v: VirtualValue[T])(implicit rt: ResultTracker): T =
    v.resolveValue.valueOption.get

  implicit def toDeflatable[T : Codec](obj: T): VirtualValue[T] =
    apply(obj)

  implicit def fromDeflatable[T : Codec](v: VirtualValue[T])(implicit rt: ResultTracker): T =
    unapply(v)
}


