package com.cibo.provenance

import java.io._
import java.nio.file.{Files, Paths}

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._
import com.cibo.io.s3.S3AsyncTransfer.calcMd5Base64
import com.cibo.io.s3._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Try}
import scala.util.control.NonFatal


/**
  * KVStore is a replacement for S3DB in this library.
  * It could evolve to possibly replace it globally, but right now it is used only here.
  */

object KVStore {
  def apply(path: String)(
    implicit s3SyncClient: AmazonS3 = com.cibo.aws.AWSClient.Implicits.s3SyncClient,
    ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  ): KVStore =
    path match {
      case p if p.startsWith("s3://") => new S3Store(p)(s3SyncClient)
      case p => new LocalStore(p)
    }
}

sealed trait KVStore {

  // public API

  def isLocal: Boolean

  def isRemote: Boolean

  def pathExists(path: String): Boolean

  def putBytes(path: String, value: Array[Byte]): Unit =
    putBytesForFullPath(getFullPathForRelativePath(path), value)

  def getBytes(path: String): Array[Byte] =
    getBytesForFullPath(getFullPathForRelativePath(path))

  def putBytesAsync(path: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] =
    putBytesForFullPathAsync(getFullPathForRelativePath(path), value)

  def getBytesAsync(path: String)(implicit ec: ExecutionContext): Future[Array[Byte]] =
    getBytesForFullPathAsync(getFullPathForRelativePath(path))

  def getSuffixes(
    prefix: String,
    filterOption: Option[String => Boolean] = None,
    delimiterOption: Option[String] = None
  ): Iterator[String] = {
    val fullPrefix = getFullPathForRelativePath(prefix)
    val offset: Int =
      if (fullPrefix.endsWith("/"))
        fullPrefix.length
      else
        fullPrefix.length + 1
    getFullPathsForPrefix(prefix, filterOption, delimiterOption).map { fullPath =>
      // This has more sanity checking that should be necessary, but one-off errors have crept in several times.
      assert(fullPath.startsWith(fullPrefix), s"Full path '$fullPath' does not start with the expected prefix '$fullPrefix' (from $prefix)")
      fullPath
        .substring(offset)
        .ensuring(!_.startsWith("/"), s"Full path suffix should not start with a '/'")
    }
  }

  // protected methods implemented by subclasses

  protected def getFullPathForRelativePath(subPrefix: String): String

  protected def putBytesForFullPath(path: String, value: Array[Byte]): Unit

  protected def getBytesForFullPath(fullPath: String): Array[Byte]

  protected def putBytesForFullPathAsync(path: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit]

  protected def getBytesForFullPathAsync(fullPath: String)(implicit ec: ExecutionContext): Future[Array[Byte]]

  protected[provenance] def remove(path: String): Unit

  protected def getSuffixesForFullPath(
    fullPath: String,
    delimiterOption: Option[String] = Some("/"),
    prevMarker: Option[String] = None,
    prevResults: Iterator[String] = Iterator.empty
  ): Iterator[String]

  // private API

  private def getFullPathsForPrefix(
    subPrefix: String,
    filterOption: Option[String => Boolean] = None,
    delimiterOption: Option[String] = None
  ): Iterator[String] = {
    // Lots of things use a subPrefix as a query and pull back a list of suffixes in a single request (caveat: paginated).
    // Note that this returns a buffered iterator and throws an exception if it is empty.
    val keyIterator = getFullPathIteratorForPath(subPrefix, delimiterOption = delimiterOption)
    filterOption.foldLeft(keyIterator)((it, f) => it.filter(f))
  }

  private def getFullPathIteratorForPath(
    path: String,
    delimiterOption: Option[String] = Some("/"),
    prevMarker: Option[String] = None,
    prevResults: Iterator[String] = Iterator.empty
  ): Iterator[String] = {
    val fullPath = getFullPathForRelativePath(path)
    getSuffixesForFullPath(fullPath, delimiterOption, prevMarker, prevResults)
  }
}

class S3Store(val baseDir: String)(implicit amazonS3: AmazonS3) extends KVStore {

  require(!baseDir.endsWith("/"), s"Found a trailing slash in ${baseDir}")
  require(!baseDir.substring(4).contains("//"), s"Found multiple consecutive slashes in ${baseDir}")

  // implement the KVStore protected API

  def isLocal: Boolean = false

  def isRemote: Boolean = true

  val (s3Bucket, s3Path): (String, String) =
    "s3://(.*?)/(.*)".r
      .findFirstMatchIn(baseDir)
      .map(head => (head.subgroups.head, head.subgroups.last))
      .getOrElse(throw new IllegalArgumentException(f"Bad s3 path: $baseDir"))

  def pathExists(path: String): Boolean = {
    val lor = new ListObjectsRequest()
    lor.setBucketName(s3Bucket)
    lor.setPrefix(s3Path + (if (path.isEmpty) "" else "/" + path))
    lor.setDelimiter("/")
    lor.setMaxKeys(1)

    import scala.collection.JavaConverters._

    Try(amazonS3.listObjects(lor)).transform(
      ol => Try {
        ol.getCommonPrefixes.asScala.contains(s3Path + "/") || ol.getObjectSummaries.asScala.map(_.getKey).contains(s3Path)
      },
      e => Failure(new RuntimeException(s"Error checking existence of $baseDir", e))
    ).get
  }
  
  protected def getFullPathForRelativePath(path: String): String =
    if (basePrefix.nonEmpty) basePrefix + "/" + path else path

  protected def putBytesForFullPath(fullPath: String, value: Array[Byte]): Unit = {
    logger.debug("putObject: checking bucket")
    createBucketIfMissing

    logger.debug(s"putObject: saving to $fullPath")

    val metadata = new ObjectMetadata()
    metadata.setContentType("application/json")
    metadata.setContentLength(value.length.toLong)
    metadata.setContentMD5(calcMd5Base64(value))

    try {
      amazonS3.putObject(bucketName, fullPath, new ByteArrayInputStream(value), metadata)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to put byte array to s3://$bucketName/$fullPath", e)
        new AccessError(s"Failed to put byte array to s3://$bucketName/$fullPath")
    }
  }

  protected def putBytesForFullPathAsync(fullPath: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.debug("putObject: checking bucket")
    createBucketIfMissing

    logger.debug(s"putObject: saving to $fullPath")

    s3async.putObject(bucketName, fullPath, value)
      .transform(
        identity[PutObjectResult],
        e => {
          logger.error(s"Failed to put byte array to s3://$bucketName/$fullPath", e)
          new AccessError(s"Failed to put byte array to s3://$bucketName/$fullPath")
        }
      ).map { _ => }
  }

  protected def getBytesForFullPath(fullPath: String): Array[Byte] = {
    createBucketIfMissing
    try {
      val obj: S3Object = amazonS3.getObject(bucketName, fullPath)
      try {
        val bis: BufferedInputStream = new java.io.BufferedInputStream(obj.getObjectContent)
        val content: Array[Byte] = Stream.continually(bis.read).takeWhile(_ != -1).map(_.toByte).toArray
        content
      } finally {
        obj.close()
      }
    } catch {
      case e: AmazonS3Exception =>
        logger.error(s"Failed to find object in bucket s3://$bucketName/$fullPath!", e)
        throw new NotFound(s"Failed to find object in bucket s3://$bucketName/$fullPath!")
      case e: Exception =>
        logger.error(s"Error accessing s3://$bucketName/$fullPath!", e)
        throw new AccessError(s"Error accessing s3://$bucketName/$fullPath!")
    }
  }

  protected def getBytesForFullPathAsync(fullPath: String)(implicit ec: ExecutionContext): Future[Array[Byte]] = {
    createBucketIfMissing
    s3async.getObject(bucketName, fullPath).transform(
      {
        obj =>
          try {
            val bis: BufferedInputStream = new java.io.BufferedInputStream(obj.getObjectContent)
            val content: Array[Byte] = Stream.continually(bis.read).takeWhile(_ != -1).map(_.toByte).toArray
            content
          } finally {
            obj.close()
          }
      },
      {
        {
          case e: AmazonS3Exception =>
            logger.error(s"Failed to find object in bucket s3://$bucketName/$fullPath!", e)
            throw new NotFound(s"Failed to find object in bucket s3://$bucketName/$fullPath!")
          case e: Exception =>
            logger.error(s"Error accessing s3://$bucketName/$fullPath!", e)
            throw new AccessError(s"Error accessing s3://$bucketName/$fullPath!")
        }
      }
    )
  }

  protected def getSuffixesForFullPath(
    fullPath: String,
    delimiterOption: Option[String] = Some("/"),
    prevMarker: Option[String] = None,
    prevResults: Iterator[String] = Iterator.empty
  ): Iterator[String] = {
    createBucketIfMissing
    val req = new ListObjectsRequest(bucketName, fullPath, prevMarker.orNull, delimiterOption.orNull, 1000)
    new S3KeyIterator(req, timeout)(amazonS3)
  }

  // private API

  private val bucketName: String = s3Bucket
  private val basePrefix: String = s3Path
  private lazy val timeout: Duration = 2.minutes
  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private def s3async(implicit ec: ExecutionContext): S3AsyncTransfer = S3AsyncTransfer.apply

  private lazy val createBucketIfMissing: Unit = {
    // This is only done once.
    val exists = try amazonS3.doesBucketExistV2(bucketName) catch {
      case NonFatal(e) =>
        logger.error(s"Error checking existence bucket $bucketName", e)
        throw new RuntimeException(s"Error checking existence bucket $bucketName", e)
    }

    if (!exists) {
      logger.info(s"Bucket $bucketName not found. Creating it now.")
      try amazonS3.createBucket(bucketName) catch {
        case NonFatal(e) =>
          logger.error(s"Error creating bucket $bucketName", e)
          throw new RuntimeException(s"Error creating bucket $bucketName", e)
      }
    }
  }

  protected[provenance] def remove(path: String): Unit = {
    amazonS3.deleteObject(s3Bucket, basePrefix  + "/" + path)
  }
}


class LocalStore(val baseDir: String) extends KVStore {

  require(!baseDir.endsWith("/"), s"Found a trailing slash in ${baseDir}")
  require(!baseDir.contains("//"), s"Found multiple consecutive slashes in ${baseDir}")

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def isLocal: Boolean = true

  def isRemote: Boolean = false

  def pathExists(path: String): Boolean = {
    val fullPath = if (path.isEmpty) baseDir else baseDir + "/" + path
    new File(fullPath).exists()
  }
  
  // the only addition to the public API is a method to destroy everything

  def cleanUp(): Unit =
    recursiveListFiles(new File(baseDir)).sortBy(_.getAbsolutePath).reverse.foreach(_.delete)

  // implement the KVStore subclass protected API

  protected def getFullPathForRelativePath(path: String): String = path

  protected def getSuffixesForFullPath(
    fullPrefix: String,
    delimiterOption: Option[String] = Some("/"),
    prevMarker: Option[String] = None,
    prevResults: Iterator[String] = Iterator.empty
  ): Iterator[String] = {
    val fsPath = getFsPathForFullPrefix(fullPrefix)
    val dir = new File(fsPath)
    val offset = baseDir.length + 1
    try {
      recursiveListFiles(dir).filter(f => !f.isDirectory).map(_.getAbsolutePath).sorted.map(_.substring(offset)).toIterator
    } catch {
      case e: Exception =>
        recursiveListFiles(dir).map(_.getAbsolutePath).sorted.map(_.substring(offset)).toIterator
    }
  }

  protected def putBytesForFullPath(fullPath: String, value: Array[Byte]): Unit = {
    val path: String = getFsPathForSubPrefix(fullPath)
    val parentDir = new File(path).getParentFile
    if (!parentDir.exists)
      parentDir.mkdirs
    val bos: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(path))
    Stream.continually(bos.write(value))
    bos.close()
  }

  protected def getBytesForFullPath(fullPath: String): Array[Byte] = {
    val fullFsPathValue: String = getFsPathForFullPrefix(fullPath)
    val file = new File(fullFsPathValue)
    if (!file.exists()) {
      throw new NotFound(s"Failed to find object at $fullPath!")
    } else

      try {
        Files.readAllBytes(Paths.get(fullFsPathValue))
      } catch {
        case e: Exception =>
          throw new AccessError(s"Error accessing $fullPath!")
      }
  }

  protected def getBytesForFullPathAsync(fullPath: String)(implicit ec: ExecutionContext): Future[Array[Byte]] =
    Future { getBytesForFullPath(fullPath) }

  protected def putBytesForFullPathAsync(fullPath: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] =
    Future { putBytesForFullPath(fullPath, value) }


  // private API

  private def getFsPathForFullPrefix(fullPrefix: String) = Seq(baseDir, fullPrefix).filter(_.nonEmpty).mkString("/")

  private def getFsPathForSubPrefix(subPrefix: String) = getFsPathForFullPrefix(getFullPathForRelativePath(subPrefix))

  private def recursiveListFiles(f: File): Array[File] = {
    if (!f.exists)
      Array.empty
    else {
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }
  }

  protected[provenance] def remove(path: String): Unit =
    new File(baseDir + "/" + path).delete()
}

class NotFound(msg: String) extends RuntimeException(msg)

class AccessError(msg: String) extends RuntimeException(msg)
