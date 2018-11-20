package com.cibo.provenance

import java.io.Serializable

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * The base trait for wrapping encoder/decoder pairs.
  *
  * The builtin encoding (JsonCodec) uses Circe JSON is used exclusively by the core library,
  * but any logic that can convert to/from an Array[Byte] works.
  *
  * @tparam T       The type of data to be encoded/decoded.
  */
trait Codec[T] extends Serializable {
  def classTag: ClassTag[T]
  def typeTag: TypeTag[T]
  def serializableClassName: String = Codec.classTagToSerializableName(classTag)
  def serialize(obj: T, checkConsistency: Boolean = true): Array[Byte]
  def deserialize(bytes: Array[Byte]): T
}