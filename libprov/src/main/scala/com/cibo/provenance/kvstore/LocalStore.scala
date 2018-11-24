package com.cibo.provenance.kvstore

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.file.{Files, Paths}

import com.cibo.provenance.exceptions.{AccessErrorException, NotFoundException}

import scala.concurrent.{ExecutionContext, Future}

case class LocalStore(basePath: String) extends KVStore {

  require(!basePath.endsWith("/"), s"Found a trailing slash in $basePath")
  require(!basePath.contains("//"), s"Found multiple consecutive slashes in $basePath")

  def isLocal: Boolean = true

  def isRemote: Boolean = false

  def exists(path: String): Boolean = {
    val absolutePath = if (path.isEmpty) basePath else basePath + "/" + path
    new File(absolutePath).exists()
  }

  def putBytes(key: String, value: Array[Byte]): Unit = {
    val path: String = getFullPathForRelativePath(key)
    val parentDir = new File(path).getParentFile
    if (!parentDir.exists)
      parentDir.mkdirs
    val bos: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(path))
    Stream.continually(bos.write(value))
    bos.close()

  }

  def getBytes(key: String): Array[Byte] = {
    val fullFsPathValue: String = getFullPathForRelativePath(key)
    val file = new File(fullFsPathValue)
    if (!file.exists()) {
      throw new NotFoundException(s"Failed to find object at $key!")
    } else
      try {
        Files.readAllBytes(Paths.get(fullFsPathValue))
      } catch {
        case e: Exception =>
          throw new AccessErrorException(s"Error accessing $key!")
      }
  }

  def putBytesAsync(key: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] =
    Future { putBytes(key, value) }

  def getBytesAsync(key: String)(implicit ec: ExecutionContext): Future[Array[Byte]] =
    Future { getBytes(key) }

  def getKeySuffixes(
    keyPrefix: String = "",
    delimiterOption: Option[String] = None
  ): Iterable[String] = {
    require(!keyPrefix.endsWith("/"), f"The $keyPrefix should not end in a slash!: $keyPrefix")

    val fullPrefix = getFullPathForRelativePath(keyPrefix)
    val dir = new File(fullPrefix)
    val offset = basePath.length + 1 + keyPrefix.length + 1

    recursiveListFiles(dir)
      .filter(f => !f.isDirectory)
      .map(_.getAbsolutePath)
      .sorted
      .map(_.substring(offset))

  }

  // the only addition to the public API is a method to destroy everything

  def cleanUp(): Unit =
    recursiveListFiles(new File(basePath)).sortBy(_.getAbsolutePath).reverse.foreach(_.delete)

  // private API

  private def getFullPathForRelativePath(absolutePath: String) = Seq(basePath, absolutePath).filter(_.nonEmpty).mkString("/")

  private def recursiveListFiles(f: File): Array[File] = {
    if (!f.exists)
      Array.empty
    else {
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }
  }

  protected[provenance] def remove(path: String): Unit =
    new File(basePath + "/" + path).delete()
}


object LocalStore {
  import io.circe._
  import io.circe.generic.semiauto._
  implicit val encoder: Encoder[LocalStore] = deriveEncoder[LocalStore]
  implicit val decoder: Decoder[LocalStore] = deriveDecoder[LocalStore]
}
