package com.cibo.provenance.kvstore

import java.io.{BufferedInputStream, ByteArrayInputStream}
import java.util
import java.util.Collections
import java.util.concurrent.{AbstractExecutorService, TimeUnit}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._
import com.cibo.provenance.{CacheUtils, Codec}
import com.cibo.provenance.exceptions.{AccessErrorException, NotFoundException}
import com.github.dwhjames.awswrap.s3.AmazonS3ScalaClient
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Try}
import scala.util.control.NonFatal


class S3Store(val basePath: String)(implicit val amazonS3: AmazonS3 = S3Store.amazonS3) extends KVStore {

  require(!basePath.endsWith("/"), s"Found a trailing slash in $basePath")
  require(!basePath.substring(4).contains("//"), s"Found multiple consecutive slashes in $basePath")

  // implement the KVStore public API

  def isLocal: Boolean = false

  def isRemote: Boolean = true

  val (s3Bucket, s3Path): (String, String) =
    "s3://(.*?)/(.*)".r
      .findFirstMatchIn(basePath)
      .map(head => (head.subgroups.head, head.subgroups.last))
      .getOrElse(throw new IllegalArgumentException(f"Bad s3 path: $basePath"))

  def exists(path: String): Boolean = {
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

  def putBytes(key: String, value: Array[Byte], contentTypeOption: Option[String] = None): Unit = {
    val fullKey = getFullS3KeyForRelativeKey(key)
    createBucketIfMissing
    val metadata = new ObjectMetadata()
    contentTypeOption match {
      case Some(contentType) => metadata.setContentType(contentType)
      case None =>
    }
    metadata.setContentLength(value.length.toLong)
    try {
      amazonS3.putObject(s3Bucket, fullKey, new ByteArrayInputStream(value), metadata)
    } catch {
      case e: Exception =>
        throw new AccessErrorException(s"Failed to put byte array to s3://$s3Bucket/$fullKey", e)
    }
  }

  def getBytes(key: String): Array[Byte] = {
    val fullKey = getFullS3KeyForRelativeKey(key)
    createBucketIfMissing
    try {
      val obj: S3Object = amazonS3.getObject(s3Bucket, fullKey)
      try {
        val bis: BufferedInputStream = new java.io.BufferedInputStream(obj.getObjectContent)
        val content: Array[Byte] = Stream.continually(bis.read).takeWhile(_ != -1).map(_.toByte).toArray
        content
      } finally {
        obj.close()
      }
    } catch {
      case e: AmazonS3Exception =>
        throw new NotFoundException(s"Failed to find object in bucket s3://$s3Bucket/$fullKey!", e)
      case e: Exception =>
        throw new AccessErrorException(s"Error accessing s3://$s3Bucket/$fullKey!", e)
    }
  }

  def putBytesAsync(key: String, value: Array[Byte], contentTypeOption: Option[String])(implicit ec: ExecutionContext): Future[PutObjectResult] = {
    val fullKey = getFullS3KeyForRelativeKey(key)
    createBucketIfMissing
    val baInputStream = new ByteArrayInputStream(value)
    val metadata = new ObjectMetadata()
    contentTypeOption match {
      case Some(contentType) => metadata.setContentType(contentType)
      case None =>
    }
    metadata.setContentLength(value.length.toLong)
    val putObjectRequest = new PutObjectRequest(s3Bucket, fullKey, baInputStream, metadata)
    S3Store.amazonS3Scala(ec).putObject(putObjectRequest).transform(
      identity[PutObjectResult],
      {
        case e: Exception => new AccessErrorException(s"Error accessing s3://$s3Bucket/$fullKey!", e)
        case other => other
      }
    )
  }


  def getBytesAsync(key: String)(implicit ec: ExecutionContext): Future[Array[Byte]] = {
    val fullKey = getFullS3KeyForRelativeKey(key)
    createBucketIfMissing
    S3Store.amazonS3Scala(ec).getObject(s3Bucket, fullKey).transform(
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
        case e: AmazonS3Exception => throw new NotFoundException(s"Failed to load object from s3://$s3Bucket/$fullKey!", e)
        case e: Exception => throw new AccessErrorException(s"Error accessing s3://$s3Bucket/$fullKey!", e)
        case other => other
      }
    )
  }

  def getSubKeys(keyPrefix: String = ""): Iterable[String] =
    getKeys(keyPrefix, recursive = false)

  def getSubKeysRecursive(keyPrefix: String = ""): Iterable[String] =
    getKeys(keyPrefix, recursive = true)

  protected def getKeys(keyPrefix: String = "", recursive: Boolean = true): Iterable[String] = {
    val fullPrefix = getFullS3KeyForRelativeKey(keyPrefix)

    val fullPrefixWithSeparator =
      if (fullPrefix.endsWith("/"))
        fullPrefix
      else
        fullPrefix + "/"

    val offset: Int = fullPrefixWithSeparator.length
    val delimit = if (recursive) None else Some("/")
    val fullKeys = S3KeyIterator(s3Bucket, fullPrefixWithSeparator, delimit)(amazonS3).toList

    fullKeys.map { fullPath =>
      // This has more sanity checking that should be necessary, but one-off errors have crept in several times.
      assert(fullPath.startsWith(fullPrefixWithSeparator), s"Full path '$fullPath' does not start with the expected prefix '$fullPrefixWithSeparator' (from $keyPrefix)")
      fullPath
        .substring(offset).stripSuffix("/")
        .ensuring(!_.startsWith("/"), s"Full path suffix should not start with a '/'")
        .ensuring(!_.endsWith("/"), s"Full path suffix should not end with a '/'")
    }
  }

  // protected API

  protected def getFullS3KeyForRelativeKey(key: String): String =
    if (s3Path.nonEmpty) s3Path + "/" + key else key

  protected[provenance] def remove(path: String): Unit = {
    amazonS3.deleteObject(s3Bucket, s3Path  + "/" + path)
  }

  protected lazy val createBucketIfMissing: Unit = {
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
}


object S3Store {
  import io.circe._

  implicit val encoder: Encoder[S3Store] =
    Encoder.forProduct1("basePath") { obj => Tuple1(obj.basePath) }

  implicit val decoder: Decoder[S3Store] =
    Decoder.forProduct1("basePath") { basePath: String => new S3Store(basePath) }

  implicit val codec: Codec[S3Store] = Codec(encoder, decoder)

  lazy val amazonS3: AmazonS3 = amazonS3Scala(scala.concurrent.ExecutionContext.global).client

  def amazonS3Scala(implicit ec: ExecutionContext): AmazonS3ScalaClient = richClientCache.get(ec)

  private val richClientCache =
    CacheUtils.mkLoadingCache(100) {
      implicit ec: ExecutionContext =>
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
            override def shutdown(): Unit = ()
            override def shutdownNow(): util.List[Runnable] = Collections.emptyList[Runnable]
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


private object S3KeyIterator {
  def apply(bucketName: String, prefix: String, delimitOption: Option[String])(implicit s3Client: AmazonS3): S3KeyIterator =
    new S3KeyIterator({
      val req = new ListObjectsRequest
      req.setBucketName(bucketName)
      req.setPrefix(prefix)
      delimitOption match {
        case Some(delimit) => req.setDelimiter(delimit)
        case None =>
      }
      req
    })
}


private class S3KeyIterator(req: ListObjectsRequest)(implicit s3client: AmazonS3) extends Iterator[String] {
  val pageIterator = new S3PageIterator(req)

  // Note: mutable
  private var nextKeys: List[String] = getNextPageKeys

  private def getNextPageKeys: List[String] = {
    if (pageIterator.hasNext) {
      val result = pageIterator.next()
      import scala.collection.JavaConverters._
      val keys = (result.getObjectSummaries.asScala.map(_.getKey) ++ result.getCommonPrefixes.asScala).toList
      keys
    }
    else {
      List.empty[String]
    }
  }

  override def hasNext: Boolean = nextKeys.nonEmpty || pageIterator.hasNext

  override def next(): String = {
    if (nextKeys.isEmpty)
      nextKeys = getNextPageKeys
    val next = nextKeys.head
    nextKeys = nextKeys.tail
    next
  }
}


class S3PageIterator(req: ListObjectsRequest)(implicit s3client: AmazonS3) extends Iterator[ObjectListing] with LazyLogging {
  private var nextObjectListing: Option[ObjectListing] =
    Some(
      try s3client.listObjects(req) catch {
        case NonFatal(e) =>
          logger.error(s"Error listing objects in s3://${req.getBucketName}/${req.getPrefix}", e)
          throw e
      }
    )

  override def hasNext: Boolean =
    nextObjectListing match {
      case None => false
      case Some(_) => true
    }

  override def next(): ObjectListing = {
    nextObjectListing match {
      case Some(list) =>
        val nextMarker = list.getNextMarker
        if (nextMarker != null) {
          val newRequest = new ListObjectsRequest(
            req.getBucketName,
            req.getPrefix,
            list.getNextMarker,
            req.getDelimiter,
            req.getMaxKeys
          )
          val nextFutureList: ObjectListing =
            try s3client.listObjects(newRequest) catch {
              case NonFatal(e) =>
                logger.error(s"Error checking existence of in s3://${newRequest.getBucketName}/${newRequest.getPrefix}", e)
                throw e
            }
          nextObjectListing = Some(nextFutureList)
        }
        else {
          nextObjectListing = None
        }
        list
      case None =>
        throw new java.util.NoSuchElementException("next on empty iterator")
    }
  }
}

