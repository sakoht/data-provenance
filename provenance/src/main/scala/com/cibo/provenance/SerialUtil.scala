package com.cibo.provenance

import com.typesafe.scalalogging.LazyLogging

/**
  * Created by ssmith on 10/8/17.
  *
  * The serialization and digest methods are outside of the tracking system,
  * since uniform digests are required across implementations.
  *
  * This utility object provides those core functions to other classes in this package.
  *
  */

object SerialUtil extends LazyLogging {
  import io.circe.parser._, io.circe.syntax._
  import java.io._
  import org.apache.commons.codec.digest.DigestUtils

  def getBytesAndDigest[T : Codec](obj: T, checkConsistency: Boolean = true): (Array[Byte], Digest) = {
    val bytes1 = serializeImpl(obj, checkConsistency)
    val digest1 = digestBytes(bytes1)
    (bytes1, digest1)
  }

  def serializeImpl[T : Codec](obj: T, checkConsistency: Boolean = true): Array[Byte] = {
    val codec = implicitly[Codec[T]]
    implicit val encoder = codec.encoder
    implicit val decoder = codec.decoder

    if (obj.isInstanceOf[ValueWithProvenanceSerializable] && codec != ValueWithProvenanceSerializable.codec) {
      println("Odd codec!")
    }

    val json: String = obj.asJson.noSpaces

    if (checkConsistency) {
      //if (json.contains("{"))
      //  println(f"OBJ:$obj\nJSON: $json")

      decode[T](json) match {
        case Right(obj2) =>
          val json2: String = obj2.asJson.noSpaces
          if (json2 != json) {
            throw new RuntimeException(f"Failure to serialize consistently!\n$obj\n$obj2\n$json\n$json2\n$json2")
          }
        case Left(e) =>
          throw e
      }
    }

    json.getBytes("UTF-8")
  }

  def deserialize[T](bytes: Array[Byte])(implicit cd: Codec[T]): T = {
    implicit val encoder = cd.encoder
    implicit val decoder = cd.decoder
    val s = new String(bytes, "UTF-8")
    decode[T](s) match {
      case Left(error) =>
        throw error
      case Right(obj) =>
        obj
    }
  }

  def digestObject[T : Codec](value: T): Digest = {
    value match {
      case _: Array[Byte] =>
        //logger.warn("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
        throw new RuntimeException("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
      case _ =>
        getBytesAndDigest(value)._2
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
        val obj3 = SerialUtil.deserializeRaw[T](bytes2)
        val bytes3 = SerialUtil.serializeRawImpl(obj3)
        val digest3 = SerialUtil.digestBytes(bytes3)
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
    SerialUtil.deserializeRaw(SerialUtil.serializeRawImpl(obj))
}



