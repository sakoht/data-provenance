package com.cibo.provenance

import java.io._
import java.nio.file.{Files, Paths}
import java.util.Collections
import java.util.concurrent.{AbstractExecutorService, TimeUnit}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model._
import com.cibo.cache.GCache
import com.github.dwhjames.awswrap.s3.AmazonS3ScalaClient
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Try}
import scala.util.control.NonFatal


/**
  * KVStore is a replacement for S3DB in this library.
  * It could evolve to possibly replace it globally, but right now it is used only here.
  */

object KVStore {

  def apply(basePath: String)(implicit s3: AmazonS3 = S3Store.s3Client): KVStore =
    basePath match {
      case p if p.startsWith("s3://") => new S3Store(p)(s3)
      case p => new LocalStore(p)
    }
}

sealed trait KVStore {

  // public API

  def basePath: String

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
    prefix: String = "",
    filterOption: Option[String => Boolean] = None,
    delimiterOption: Option[String] = None
  ): Iterator[String] = {
    val fullPrefix = getFullPathForRelativePath(prefix)
    val offset: Int =
      if (fullPrefix.endsWith("/"))
        fullPrefix.length
      else
        fullPrefix.length + 1
    getFullPathsForRelativePathWithFilter(prefix, filterOption, delimiterOption).map { fullPath =>
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
    delimiterOption: Option[String] = Some("/")
  ): Iterator[String]

  // private API

  private def getFullPathsForRelativePathWithFilter(
    relativePath: String,
    filterOption: Option[String => Boolean] = None,
    delimiterOption: Option[String] = None
  ): Iterator[String] = {
    val keyIterator = getFullPathsForRelativePath(relativePath, delimiterOption = delimiterOption)
    filterOption.foldLeft(keyIterator)((it, f) => it.filter(f))
  }

  private def getFullPathsForRelativePath(relativePath: String, delimiterOption: Option[String] = Some("/")): Iterator[String] = {
    val fullPath = getFullPathForRelativePath(relativePath)
    getSuffixesForFullPath(fullPath, delimiterOption)
  }
}

class S3Store(val basePath: String)(implicit val amazonS3: AmazonS3) extends KVStore {

  require(!basePath.endsWith("/"), s"Found a trailing slash in ${basePath}")
  require(!basePath.substring(4).contains("//"), s"Found multiple consecutive slashes in ${basePath}")

  // implement the KVStore public API

  def isLocal: Boolean = false

  def isRemote: Boolean = true

  val (s3Bucket, s3Path): (String, String) =
    "s3://(.*?)/(.*)".r
      .findFirstMatchIn(basePath)
      .map(head => (head.subgroups.head, head.subgroups.last))
      .getOrElse(throw new IllegalArgumentException(f"Bad s3 path: $basePath"))

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
      e => Failure(new RuntimeException(s"Error checking existence of $basePath", e))
    ).get
  }

  // the KVStore protected API

  protected def getFullPathForRelativePath(path: String): String =
    if (s3Path.nonEmpty) s3Path + "/" + path else path

  protected def putBytesForFullPath(fullPath: String, value: Array[Byte]): Unit = {
    logger.debug("putObject: checking bucket")
    createBucketIfMissing

    logger.debug(s"putObject: saving to $fullPath")

    val metadata = new ObjectMetadata()
    metadata.setContentType("application/json")
    metadata.setContentLength(value.length.toLong)

    try {
      amazonS3.putObject(s3Bucket, fullPath, new ByteArrayInputStream(value), metadata)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to put byte array to s3://$s3Bucket/$fullPath", e)
        new AccessError(s"Failed to put byte array to s3://$s3Bucket/$fullPath")
    }
  }

  protected def putBytesForFullPathAsync(fullPath: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.debug("putObject: checking bucket")
    createBucketIfMissing

    logger.debug(s"putObject: saving to $fullPath")

    putObject(s3Bucket, fullPath, value)
      .transform(
        identity[PutObjectResult],
        e => {
          logger.error(s"Failed to put byte array to s3://$s3Bucket/$fullPath", e)
          new AccessError(s"Failed to put byte array to s3://$s3Bucket/$fullPath")
        }
      ).map { _ => }
  }


  private def putObject(bucket: String, key: String, objContentBytes: Array[Byte])(implicit ec: ExecutionContext): Future[PutObjectResult] = {
    val baInputStream = new ByteArrayInputStream(objContentBytes)
    val metadata = new ObjectMetadata()
    metadata.setContentType("application/json")
    metadata.setContentLength(objContentBytes.length.toLong)

    val putObjectRequest = new PutObjectRequest(bucket, key, baInputStream, metadata)

    S3Store.richClient(ec).putObject(putObjectRequest).transform(
      identity[PutObjectResult],
      t => {
        logger.error(s"Failed to write byte array to s3://$bucket/$key", t)
        t
      }
    )
  }

  protected def getBytesForFullPath(fullPath: String): Array[Byte] = {
    createBucketIfMissing
    try {
      val obj: S3Object = amazonS3.getObject(s3Bucket, fullPath)
      try {
        val bis: BufferedInputStream = new java.io.BufferedInputStream(obj.getObjectContent)
        val content: Array[Byte] = Stream.continually(bis.read).takeWhile(_ != -1).map(_.toByte).toArray
        content
      } finally {
        obj.close()
      }
    } catch {
      case e: AmazonS3Exception =>
        logger.error(s"Failed to find object in bucket s3://$s3Bucket/$fullPath!", e)
        throw new NotFound(s"Failed to find object in bucket s3://$s3Bucket/$fullPath!")
      case e: Exception =>
        logger.error(s"Error accessing s3://$s3Bucket/$fullPath!", e)
        throw new AccessError(s"Error accessing s3://$s3Bucket/$fullPath!")
    }
  }

  protected def getBytesForFullPathAsync(fullPath: String)(implicit ec: ExecutionContext): Future[Array[Byte]] = {
    createBucketIfMissing
    S3Store.richClient(ec).getObject(s3Bucket, fullPath).transform(
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
            logger.error(s"Failed to find object in bucket s3://$s3Bucket/$fullPath!", e)
            throw new NotFound(s"Failed to find object in bucket s3://$s3Bucket/$fullPath!")
          case e: Exception =>
            logger.error(s"Error accessing s3://$s3Bucket/$fullPath!", e)
            throw new AccessError(s"Error accessing s3://$s3Bucket/$fullPath!")
        }
      }
    )
  }

  protected def getSuffixesForFullPath(
    fullPath: String,
    delimiterOption: Option[String] = Some("/")
  ): Iterator[String] = {
    import scala.collection.JavaConverters._
    S3Objects.withPrefix(amazonS3, s3Bucket, fullPath).iterator().asScala.map(_.getKey())
  }

  // private API

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val createBucketIfMissing: Unit = {
    // This is only done once.
    val exists = try amazonS3.doesBucketExistV2(s3Bucket) catch {
      case NonFatal(e) =>
        logger.error(s"Error checking existence bucket $s3Bucket", e)
        throw new RuntimeException(s"Error checking existence bucket $s3Bucket", e)
    }

    if (!exists) {
      logger.info(s"Bucket $s3Bucket not found. Creating it now.")
      try amazonS3.createBucket(s3Bucket) catch {
        case NonFatal(e) =>
          logger.error(s"Error creating bucket $s3Bucket", e)
          throw new RuntimeException(s"Error creating bucket $s3Bucket", e)
      }
    }
  }

  protected[provenance] def remove(path: String): Unit = {
    amazonS3.deleteObject(s3Bucket, s3Path  + "/" + path)
  }
}

object S3Store {

  // Note: The regular sync client uses the async one to construct, but doesn't actually use the EC.
  // Supply the global one since it doesn't matter in this sceneario.
  // All methods that use an EC take it implicitly directly.  Nothing holds an EC internally.
  lazy val s3Client: AmazonS3 = richClient(scala.concurrent.ExecutionContext.global).client

  def richClient(implicit ec: ExecutionContext): AmazonS3ScalaClient = richClientCache.get(ec)

  private val richClientCache = GCache().apply { implicit ec: ExecutionContext =>
    // Set buffer size hint assuming 100 ms round trip, 100 MBps connection
    val bufferSize = (0.100 * 100000000/8).toInt

    val s3CredsProvider: DefaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain()

    val region: Regions = Regions.US_EAST_1

    val clientConfiguration: ClientConfiguration = new ClientConfiguration()
      .withMaxConnections(Runtime.getRuntime.availableProcessors * 10)
      .withMaxErrorRetry (10)
      .withConnectionTimeout (10 * 60 * 1000)
      .withSocketTimeout (10 * 60 * 1000)
      .withTcpKeepAlive(true)
      .withSocketBufferSizeHints(bufferSize, bufferSize)


    val executorService: ExecutionContextExecutorService =
      new AbstractExecutorService with ExecutionContextExecutorService {
        override def prepare(): ExecutionContext = ec
        override def isShutdown = false
        override def isTerminated = false
        override def shutdown() = ()
        override def shutdownNow() = Collections.emptyList[Runnable]
        override def execute(runnable: Runnable): Unit = ec execute runnable
        override def reportFailure(t: Throwable): Unit = ec reportFailure t
        override def awaitTermination(length: Long, unit: TimeUnit): Boolean = false
      }

    new AmazonS3ScalaClient(
      awsCredentialsProvider = s3CredsProvider,
      clientConfiguration = clientConfiguration,
      region = Some(region),
      endpointConfiguration = None,
      clientOptions = None,
      executorService = executorService
    )
  }
}


class LocalStore(val basePath: String) extends KVStore {

  require(!basePath.endsWith("/"), s"Found a trailing slash in ${basePath}")
  require(!basePath.contains("//"), s"Found multiple consecutive slashes in ${basePath}")

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def isLocal: Boolean = true

  def isRemote: Boolean = false

  def pathExists(path: String): Boolean = {
    val fullPath = if (path.isEmpty) basePath else basePath + "/" + path
    new File(fullPath).exists()
  }

  // the only addition to the public API is a method to destroy everything

  def cleanUp(): Unit =
    recursiveListFiles(new File(basePath)).sortBy(_.getAbsolutePath).reverse.foreach(_.delete)

  // implement the KVStore subclass protected API

  protected def getFullPathForRelativePath(path: String): String = path

  protected def getSuffixesForFullPath(
    fullPrefix: String,
    delimiterOption: Option[String] = Some("/")
  ): Iterator[String] = {
    val fsPath = getFsPathForFullPrefix(fullPrefix)
    val dir = new File(fsPath)
    val offset = basePath.length + 1
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

  private def getFsPathForFullPrefix(fullPrefix: String) = Seq(basePath, fullPrefix).filter(_.nonEmpty).mkString("/")

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
    new File(basePath + "/" + path).delete()
}

class NotFound(msg: String) extends RuntimeException(msg)

class AccessError(msg: String) extends RuntimeException(msg)
