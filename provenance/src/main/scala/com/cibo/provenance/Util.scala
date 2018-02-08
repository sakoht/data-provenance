package com.cibo.provenance

import java.io._

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.digest.DigestUtils

import scala.reflect.ClassTag

/**
  * Created by ssmith on 10/8/17.
  *
  * The serialization and digest methods are outside of the tracking system,
  * since uniform digests are required across implementations.
  *
  * This utility object provides those core functions to other classes in this package.
  *
  */

object Util extends LazyLogging {

  import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

  def serialize[T](obj: T)(implicit e: io.circe.Encoder[T], d: io.circe.Decoder[T]): Array[Byte] =
    getBytesAndDigest(obj)._1

  def getBytesAndDigest[T](obj: T)(implicit e: io.circe.Encoder[T], d: io.circe.Decoder[T]): (Array[Byte], Digest) = {
    val bytes1 = serialize1(obj)
    val digest1 = digestBytes(bytes1)
    val obj2 = deserialize[T](bytes1)
    val bytes2 = serialize1(obj2)
    val digest2 = digestBytes(bytes2)
    if (digest2 != digest1) {
      val obj3 = Util.deserialize[T](bytes2)
      val bytes3 = Util.serialize1(obj3)
      val digest3 = Util.digestBytes(bytes3)
      if (digest3 == digest2)
        logger.warn(f"The re-constituted version of $obj digests differently $digest1 -> $digest2!  But the reconstituted object saves consistently.")
      else
        throw new RuntimeException(f"Object $obj digests as $digest1, re-digests as $digest2 and $digest3!")
    }
    (bytes2, digest2)
  }

  def serialize1[T](obj: T)(implicit e: io.circe.Encoder[T], d: io.circe.Decoder[T]): Array[Byte] = {
    val json: String = obj.asJson.noSpaces
    val bytes = json.getBytes("UTF-8")

    // Re-parse and decode to ensure this will really round-trip.
    val json2: String = new String(bytes)
    if (json2 != json) {
      throw new RuntimeException(f"Failure to serialize consistently!\n$obj\n$json\n$json2")
    }
    decode[T](json2) match {
      case Right(obj2) =>
        val json3: String = obj2.asJson.noSpaces
        if (json3 != json) {
          throw new RuntimeException(f"Failure to serialize consistently!\n$obj\n$obj2\n$json\n$json2\n$json3")
        }
      case Left(e) =>
        throw e
    }

    bytes
  }

  def serializeRaw[T](obj: T): Array[Byte] =
    getBytesAndDigestRaw(obj)._1

  def getBytesAndDigestRaw[T](obj: T): (Array[Byte], Digest) = {
    val bytes1 = serializeRaw1(obj)
    val digest1 = digestBytes(bytes1)
    val obj2 = deserializeRaw[T](bytes1)
    val bytes2 = serializeRaw1(obj2)
    val digest2 = digestBytes(bytes2)
    if (digest2 != digest1) {
      val obj3 = Util.deserializeRaw[T](bytes2)
      val bytes3 = Util.serializeRaw1(obj3)
      val digest3 = Util.digestBytes(bytes3)
      if (digest3 == digest2)
        logger.warn(f"The re-constituted version of $obj digests differently $digest1 -> $digest2!  But the reconstituted object saves consistently.")
      else
        throw new RuntimeException(f"Object $obj digests as $digest1, re-digests as $digest2 and $digest3!")
    }
    (bytes2, digest2)
  }

  def serializeRaw1[T](obj: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.toByteArray
  }
  
  def deserialize[T](bytes: Array[Byte])(implicit e: io.circe.Encoder[T], d: io.circe.Decoder[T]): T = {
    val s = new String(bytes, "UTF-8")
    decode[T](s) match {
      case Left(error) =>
        val x = decode[T](s)
        println(x)
        throw error
      case Right(obj) =>
        obj
    }
  }

  def deserializeRaw[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    val obj1: AnyRef = ois.readObject
    val obj: T = obj1.asInstanceOf[T]
    ois.close()
    obj
  }


  def digestObject[T : ClassTag : Encoder : Decoder](value: T): Digest = {
    value match {
      case _: Array[Byte] =>
        //logger.warn("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
        throw new RuntimeException("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
      case _ =>
        getBytesAndDigest(value)._2
    }
  }

  def digestObjectRaw[T : ClassTag](value: T): Digest = {
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

  // Digest check functions

  /*
  sealed trait Problem[T]
  case class InconsistentDigestProblem[T](obj: T, attr: String) extends Problem[T]
  case class UnserializableProblem[T](obj: T, attr: String) extends Problem[T]

  def checkSerialization[T : Encoder : Decoder](obj: T, prefix: String = ""): List[Problem[_]] = {
    val childProblems: List[Util.Problem[_]] =
      obj match {
        case p: Product =>
          val parts = p.productIterator.toList
          val problems = parts.zipWithIndex.flatMap {
            case (part: Any, i: Int) =>
              checkSerialization(part, prefix + "." + i.toString)
          }
          problems.toList
        case _ =>
          Nil
      }

    try {
      if (hasSerializationSymmetry(obj, prefix)) {
        childProblems
      } else {
        InconsistentDigestProblem[T](obj, prefix) +: childProblems
      }
    } catch {
      case e: java.io.NotSerializableException =>
        UnserializableProblem[T](obj, prefix) +: childProblems
    }
  }

  def hasSerializationSymmetry[T : Encoder : Decoder](obj: T, prefix: String): Boolean = {
    val bytes1 = Util.serialize(obj)
    val digest1 = Util.digestBytes(bytes1)
    //val os = obj.toString
    //val os2 = if (os.size > 100) os.substring(0, 100) else os
    //println(prefix + " : " + digest1 + " for " + os2)
    val obj2 = Util.deserialize[T](bytes1)
    val bytes2 = Util.serialize(obj2)
    val digest2 = Util.digestBytes(bytes2)
    digest2 == digest1
  }
  */

  def getObj[O](bytes: Array[Byte], foo: Int): O = {
    try {
      Util.deserializeRaw(bytes)
    } catch {
      case e: Exception =>
        Util.deserializeRaw(bytes)
    }
  }

  def rawDecoder[T]: Decoder[T] = {
    val d: Decoder[T] = Decoder.forProduct2("bytes", "foo")(getObj[T])
    d
  }

  def rawEncoder[T]: Encoder[T] = {
    Encoder.forProduct2("bytes", "foo") {
      obj =>
        val bytes = Util.serializeRaw(obj)
        Tuple2(bytes, bytes.length)
    }
  }

}
