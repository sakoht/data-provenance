package com.cibo.provenance

import java.io.Serializable

import com.cibo.provenance.Codec.logger
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, Encoder, Json}

import scala.reflect.ClassTag
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.TypeTag

/**
  * The base trait for wrapping encoder/decoder pairs.
  *
  * The builtin encoding (CodecUsingJson) uses Circe JSON is used exclusively by the core library,
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


/**
  * Serialization-related utility methods.
  */
object Codec extends LazyLogging {
  import scala.language.existentials
  import scala.language.implicitConversions
  import scala.language.reflectiveCalls
  import scala.tools.reflect.ToolBox

  import java.io._
  import org.apache.commons.codec.digest.DigestUtils
  import scala.util.{Try, Failure, Success}

  /**
    * A Codec of sub-class JSON Codec can always be explicitly constructed from a pair of circe encoders,
    * presuming the ClassTag and TypeTag are available.
    *
    * @tparam T The type of data to be encoded/decoded.
    * @param encoder  A circe Encoder[T].
    * @param decoder  A circe Decoder[T].
    * @return A Codec[T] created from implicits.
    */
  def apply[T : ClassTag : TypeTag](encoder: io.circe.Encoder[T], decoder: io.circe.Decoder[T]): CodecUsingJson[T] =
    CodecUsingJson(encoder, decoder)

  /**
    * Implicitly create a Codec[T] wherever an Encoder[T] and Decoder[T] are implicitly available.
    * These can be provided by io.circe.generic.semiauto._ w/o penalty, or .auto._ with some penalty.
    *
    * @tparam   T The type of data to be encoded/decoded.
    * @return A Codec[T] created from implicits.
    */
  implicit def createCodec[T : Encoder : Decoder : ClassTag : TypeTag]: Codec[T] =
    Codec(implicitly[Encoder[T]], implicitly[Decoder[T]])

  /**
    * Normally you can only auto-derive a codec for an abstract class if it is a sealed
    * trait, since deserialization cannot be aware of an open-ended collection of subtypes.
    *
    * Create a Codec[T] for any abstract class/trait, presuming all subclasses
    * have a companion object that can return an encoder and decoder.
    *
    * NOTE: Because this cannot be verified at compile time a runtime exception will occur
    * during deserialization if the named class does not have a companion object with
    * the required methods.
    *
    * @param key                    The JSON object key that holds the subclass (defaults to "_subclass".)
    * @param valueStringToClassName A function to turn the key value into a class name during decode.
    * @param classNameToValueString A function to turn the class name into a string.
    * @tparam T   The base class/trait.
    * @return     A Codec[T]
    */
  def createAbstractCodec[T : ClassTag : TypeTag](
    key: String = "_subclass",
    valueStringToClassName: (String) => String = (className: String) => className,
    classNameToValueString: (String) => String = (className: String) => className
  ): CodecUsingJson[T] = {
    type COMPANION = { def encoder: Encoder[T]; def decoder: Decoder[T] }

    val encoder = Encoder.instance {
      o: T =>
        val className = o.getClass.getName.stripSuffix("$")
        val valueString = classNameToValueString(className)
        val companionObject: COMPANION = objFromSerializableName[COMPANION](className)
        val encoder = companionObject.encoder
        encoder.apply(o).mapObject(
          o1 => o1.add(key, Json.fromString(valueString))
        )
    }

    val decoder = Decoder.instance(
      cursor => {
        cursor.downField(key).as[String] match {
          case Right(valueString) =>
            val className = valueStringToClassName(valueString)
            val companionObject: COMPANION  = objFromSerializableName[COMPANION](className)
            cursor.as(companionObject.decoder)
          case Left(err) =>
            throw err
        }
      }
    )

    Codec(encoder, decoder)
  }

  /**
    * Implicitly create a Codec[ Codec[T] ] so we can serialize/deserialize Codecs.
    * This allows us to fully round-trip data across processes.
    * Note that the Codecs do not themselves serialize reproducibly.
    *
    * @tparam T   The type of data underlying the underlying Codec
    * @return     a Codec[ Codec[T] ]
    */
  implicit def selfCodec[T : ClassTag](implicit tt: TypeTag[Codec[T]], ct: ClassTag[Codec[T]]): Codec[Codec[T]] =
    Codec(new BinaryEncoder[Codec[T]], new BinaryDecoder[Codec[T]])

  /**
    * Implicitly convert a Codec[T] into a TypeTag[T].
    *
    * @param codec
    * @tparam T
    * @return
    */
  implicit def toTypeTag[T](codec: Codec[T]): TypeTag[T] = codec.typeTag

  /**
    * Implicitly convert a Codec[T] into a ClassTag[T].
    *
    * @param codec
    * @tparam T
    * @return
    */
  implicit def toClassTag[T](codec: Codec[T]): ClassTag[T] = codec.classTag

  /**
    * Convert a ClassTag[T] into the name to be used for serialization.
    * @param ct
    * @tparam T
    * @return
    */
  def classTagToSerializableName[T](implicit ct: ClassTag[T]) = {
    val name1 = ct.toString
    Try(Class.forName(name1)) match {
      case Success(_) => name1
      case Failure(_) =>
        val name2 = "scala." + name1
        Try(Class.forName(name2)) match {
          case Success(_) => name2
          case Failure(_) =>
            throw new RuntimeException(f"Failed to resolve a class name for $ct")
        }
    }
  }

  val toolbox = currentMirror.mkToolBox()

  def functionFromSerializableName(name: String): FunctionWithProvenance[_] = {
    try {
      // This will work if the function is a singleton.
      val clazz = Class.forName(name + "$")
      val obj = clazz.getField("MODULE$").get(clazz)
      obj.asInstanceOf[FunctionWithProvenance[_]]
    } catch {
      case e: Exception if e.isInstanceOf[java.lang.ClassCastException] | e.isInstanceOf[java.lang.ClassNotFoundException] =>
        // We fall back to using the compiler itself only when re-vivifying a function object as data,
        // _and_ the function is itself parameterized.  Normal workflow activity never goes here, only
        // introspection.
        if (!name.contains("[")) {
          throw new RuntimeException(
            f"Error loading FunctionWithProvenance $name: " +
              f"should either be a singleton or a class with type parameters in the name and no args to construct!"
          )
        }
        val tree = toolbox.parse("new " + name)
        val fwp = toolbox.compile(tree).apply()
        fwp.asInstanceOf[FunctionWithProvenance[_]]
    }
  }

  def objectFromSerializableName[T](name: String): T = {
    try {
      // This will work if the function is a singleton.
      val clazz = Class.forName(name + "$")
      val obj = clazz.getField("MODULE$").get(clazz)
      obj.asInstanceOf[T]
    } catch {
      case e: Exception if e.isInstanceOf[java.lang.ClassCastException] | e.isInstanceOf[java.lang.ClassNotFoundException] =>
        // We fall back to using the compiler itself only when re-vivifying a function object as data,
        // _and_ the function is itself parameterized.  Normal workflow activity never goes here, only
        // introspection.
        if (!name.contains("[")) {
          throw new RuntimeException(
            f"Error loading obj $name: " +
              f"should either be a singleton or a class with type parameters in the name and no args to construct!"
          )
        }
        val tree = toolbox.parse("new " + name)
        val obj = toolbox.compile(tree).apply()
        obj.asInstanceOf[T]
    }
  }

  def objFromSerializableName[T](name: String): T = {
    val clazz = Class.forName(name + "$")
    val obj = clazz.getField("MODULE$").get(clazz)
    obj.asInstanceOf[T]
  }

  /**
    * Return the bytes and digest value of an object that has an implicit Codec.
    * Note that this is more efficient than requesting them separately because each uses the other.
    *
    * @param obj              The object to serialize.
    * @param checkConsistency A boolean flag set to true by default.
    * @tparam T
    * @return                 A byte array and a digest value.
    */
  def serializeAndDigest[T : Codec](obj: T, checkConsistency: Boolean = true): (Array[Byte], Digest) = {
    val bytes = serialize(obj, checkConsistency)
    val digest = digestBytes(bytes)
    (bytes, digest)
  }

  def serialize[T : Codec](obj: T, checkConsistency: Boolean = true): Array[Byte] =
    implicitly[Codec[T]].serialize(obj, checkConsistency)

  def deserialize[T : Codec](bytes: Array[Byte]): T =
    implicitly[Codec[T]].deserialize(bytes)

  def digestObject[T : Codec](value: T): Digest = {
    value match {
      case _: Array[Byte] =>
        //logger.warn("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
        throw new RuntimeException("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
      case _ =>
        serializeAndDigest(value)._2
    }
  }

  def getBytesAndDigestRaw[T](obj: T, checkConsistency: Boolean = true): (Array[Byte], Digest) = {
    val bytes1 = serializeRawImpl(obj)
    val digest1 = digestBytes(bytes1)
    if (checkConsistency) {
      val obj2 = deserializeRaw[T](bytes1)
      val bytes2 = serializeRawImpl(obj2)
      val digest2 = digestBytes(bytes2)
      if (digest2 != digest1) {
        val obj3 = Codec.deserializeRaw[T](bytes2)
        val bytes3 = Codec.serializeRawImpl(obj3)
        val digest3 = Codec.digestBytes(bytes3)
        if (digest3 == digest2)
          logger.warn(f"The re-constituted (bytes) version of $obj digests differently $digest1 -> $digest2!  But the reconstituted object saves consistently.")
        else
          throw new RuntimeException(f"Object $obj digests as $digest1, re-digests as $digest2 and $digest3!")
      }
      (bytes2, digest2)
    } else {
      (bytes1, digest1)
    }
  }

  def serializeRawImpl[T](obj: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.toByteArray
  }

  def deserializeRaw[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    val obj1: AnyRef = ois.readObject
    val obj: T = obj1.asInstanceOf[T]
    ois.close()
    obj
  }

  def digestObjectRaw[T : Codec](value: T): Digest = {
    value match {
      case _: Array[Byte] =>
        //logger.warn("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
        throw new RuntimeException("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
      case _ =>
        getBytesAndDigestRaw(value)._2
    }
  }

  def digestBytes(bytes: Array[Byte]): Digest =
    Digest(DigestUtils.sha1Hex(bytes))

  def clean[T : Codec](obj: T): T =
    Codec.deserializeRaw(Codec.serializeRawImpl(obj))
}
