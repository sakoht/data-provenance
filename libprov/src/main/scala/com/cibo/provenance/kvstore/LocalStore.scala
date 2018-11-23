package com.cibo.provenance.kvstore

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.file.{Files, Paths}

import com.cibo.provenance.exceptions.{AccessErrorException, NotFoundException}

import scala.concurrent.{ExecutionContext, Future}

case class LocalStore(val basePath: String) extends KVStore {

  require(!basePath.endsWith("/"), s"Found a trailing slash in ${basePath}")
  require(!basePath.contains("//"), s"Found multiple consecutive slashes in ${basePath}")

  def isLocal: Boolean = true

  def isRemote: Boolean = false

  def exists(path: String): Boolean = {
    val absolutePath = if (path.isEmpty) basePath else basePath + "/" + path
    new File(absolutePath).exists()
  }

  // the only addition to the public API is a method to destroy everything

  def cleanUp(): Unit =
    recursiveListFiles(new File(basePath)).sortBy(_.getAbsolutePath).reverse.foreach(_.delete)

  // implement the KVStore subclass protected API

  /**
    * Translate a (relative) path in the KVStore into a full "key".
    * In a LocalStore the "key" is a full local filesystem path.
    * 
    * @param key   
    * @return       The full local filesystem path for a path in the KVStore.
    */
  protected def getAbsolutePathForKey(key: String): String = key

  protected def getAbsolutePathsForAbsolutePrefix(
    absolutePrefix: String,
    delimiterOption: Option[String] = Some("/")
  ): Iterator[String] = {
    val fsPath = getFsPathForAbsolutePath(absolutePrefix)
    val dir = new File(fsPath)
    val offset = basePath.length + 1
    try {
      recursiveListFiles(dir).filter(f => !f.isDirectory).map(_.getAbsolutePath).sorted.map(_.substring(offset)).toIterator
    } catch {
      case e: Exception =>
        recursiveListFiles(dir).map(_.getAbsolutePath).sorted.map(_.substring(offset)).toIterator
    }
  }

  protected def putBytesForAbsolutePath(absolutePath: String, value: Array[Byte]): Unit = {
    val path: String = getFsPathForKey(absolutePath)
    val parentDir = new File(path).getParentFile
    if (!parentDir.exists)
      parentDir.mkdirs
    val bos: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(path))
    Stream.continually(bos.write(value))
    bos.close()
  }

  protected def getBytesForAbsolutePath(absolutePath: String): Array[Byte] = {
    val fullFsPathValue: String = getFsPathForAbsolutePath(absolutePath)
    val file = new File(fullFsPathValue)
    if (!file.exists()) {
      throw new NotFoundException(s"Failed to find object at $absolutePath!")
    } else

      try {
        Files.readAllBytes(Paths.get(fullFsPathValue))
      } catch {
        case e: Exception =>
          throw new AccessErrorException(s"Error accessing $absolutePath!")
      }
  }

  protected def getBytesForAbsolutePathAsync(absolutePath: String)(implicit ec: ExecutionContext): Future[Array[Byte]] =
    Future { getBytesForAbsolutePath(absolutePath) }

  protected def putBytesForAbsolutePathAsync(absolutePath: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] =
    Future { putBytesForAbsolutePath(absolutePath, value) }


  // private API

  private def getFsPathForAbsolutePath(absolutePath: String) = Seq(basePath, absolutePath).filter(_.nonEmpty).mkString("/")

  private def getFsPathForKey(key: String) = getFsPathForAbsolutePath(getAbsolutePathForKey(key))

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
