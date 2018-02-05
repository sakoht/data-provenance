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

  def getBytesAndDigest[T](obj: T): (Array[Byte], Digest) = {
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

  def serialize[T](obj: T): Array[Byte] = getBytesAndDigest(obj)._1

  def serialize0[T](obj: T): Array[Byte] = {

    ???
  }

  def serialize1[T](obj: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.toByteArray
  }

  def saveBytes(bytes: Array[Byte], file: File): Unit = {
    val parentDir = file.getParentFile
    if (!parentDir.exists)
      parentDir.mkdirs()
    val fos = new FileOutputStream(file)
    fos.write(bytes)
    fos.close()
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    val obj1: AnyRef = ois.readObject
    val obj: T = obj1.asInstanceOf[T]
    ois.close()
    obj
  }

  def deserialize[T](file: File): T = {
    val bais = new FileInputStream(file)
    val ois = new ObjectInputStream(bais)
    val obj1: AnyRef = ois.readObject
    val obj: T = obj1.asInstanceOf[T]
    ois.close()
    obj
  }

  def digestObject[T : ClassTag](value: T): Digest = {
    value match {
      case _: Array[Byte] =>
        //logger.warn("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
        throw new RuntimeException("Attempt to digest a byte array.  Maybe you want to digest the bytes no the serialized object?")
      case _ =>
        getBytesAndDigest(value)._2
    }
  }

  def digestBytes(bytes: Array[Byte]): Digest =
    Digest(DigestUtils.sha1Hex(bytes))

  // Digest check functions

  sealed trait Problem[T]
  case class InconsistentDigestProblem[T](obj: T, attr: String) extends Problem[T]
  case class UnserializableProblem[T](obj: T, attr: String) extends Problem[T]

  def checkSerialization[T](obj: T, prefix: String = ""): List[Problem[_]] = {
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

  def hasSerializationSymmetry[T](obj: T, prefix: String): Boolean = {
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

}
