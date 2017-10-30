package com.cibo.provenance

import java.io._

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

object Util {

  def serialize[T](obj: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.toByteArray
  }

  def serialize[T](obj: T, file: File): Unit = {
    obj match {
      case _: Array[Byte] => throw new RuntimeException("Input object is already byte array?")
      case _ =>
    }
    val parentDir = file.getParentFile
    if (!parentDir.exists)
      parentDir.mkdirs()
    val baos = new FileOutputStream(file)
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
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

  def digest[T : ClassTag](value: T): Digest =
    digestBytes(serialize(value))

  def digestBytes(bytes: Array[Byte]): Digest =
    Digest(DigestUtils.sha1Hex(bytes))

}
