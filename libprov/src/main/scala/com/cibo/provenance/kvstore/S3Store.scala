package com.cibo.provenance.kvstore

import java.io.{BufferedInputStream, ByteArrayInputStream}
import java.util.Collections
import java.util.concurrent.{AbstractExecutorService, TimeUnit}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model._
import com.cibo.provenance.{BuildInfoGit, BuildInfoGitSaved, CacheUtils, Codec}
import com.cibo.provenance.exceptions.{AccessErrorException, NotFoundException}
import com.github.dwhjames.awswrap.s3.AmazonS3ScalaClient

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Try}
import scala.util.control.NonFatal


class S3Store(val basePath: String)(implicit val amazonS3: AmazonS3 = S3Store.s3Client) extends KVStore {

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

  // the KVStore protected API

  protected def getAbsolutePathForKey(key: String): String =
    if (s3Path.nonEmpty) s3Path + "/" + key else key

  protected def putBytesForAbsolutePath(absolutePath: String, value: Array[Byte]): Unit = {
    logger.debug("putObject: checking bucket")
    createBucketIfMissing

    logger.debug(s"putObject: saving to $absolutePath")

    val metadata = new ObjectMetadata()
    metadata.setContentType("application/json")
    metadata.setContentLength(value.length.toLong)

    try {
      amazonS3.putObject(s3Bucket, absolutePath, new ByteArrayInputStream(value), metadata)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to put byte array to s3://$s3Bucket/$absolutePath", e)
        new AccessErrorException(s"Failed to put byte array to s3://$s3Bucket/$absolutePath")
    }
  }

  protected def putBytesForAbsolutePathAsync(absolutePath: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.debug("putObject: checking bucket")
    createBucketIfMissing

    logger.debug(s"putObject: saving to $absolutePath")

    putObject(s3Bucket, absolutePath, value)
      .transform(
        identity[PutObjectResult],
        e => {
          logger.error(s"Failed to put byte array to s3://$s3Bucket/$absolutePath", e)
          new AccessErrorException(s"Failed to put byte array to s3://$s3Bucket/$absolutePath")
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

  protected def getBytesForAbsolutePath(absolutePath: String): Array[Byte] = {
    createBucketIfMissing
    try {
      val obj: S3Object = amazonS3.getObject(s3Bucket, absolutePath)
      try {
        val bis: BufferedInputStream = new java.io.BufferedInputStream(obj.getObjectContent)
        val content: Array[Byte] = Stream.continually(bis.read).takeWhile(_ != -1).map(_.toByte).toArray
        content
      } finally {
        obj.close()
      }
    } catch {
      case e: AmazonS3Exception =>
        logger.error(s"Failed to find object in bucket s3://$s3Bucket/$absolutePath!", e)
        throw new NotFoundException(s"Failed to find object in bucket s3://$s3Bucket/$absolutePath!")
      case e: Exception =>
        logger.error(s"Error accessing s3://$s3Bucket/$absolutePath!", e)
        throw new AccessErrorException(s"Error accessing s3://$s3Bucket/$absolutePath!")
    }
  }

  protected def getBytesForAbsolutePathAsync(absolutePath: String)(implicit ec: ExecutionContext): Future[Array[Byte]] = {
    createBucketIfMissing
    S3Store.richClient(ec).getObject(s3Bucket, absolutePath).transform(
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
            logger.error(s"Failed to find object in bucket s3://$s3Bucket/$absolutePath!", e)
            throw new NotFoundException(s"Failed to find object in bucket s3://$s3Bucket/$absolutePath!")
          case e: Exception =>
            logger.error(s"Error accessing s3://$s3Bucket/$absolutePath!", e)
            throw new AccessErrorException(s"Error accessing s3://$s3Bucket/$absolutePath!")
        }
      }
    )
  }

  protected def getAbsolutePathsForAbsolutePrefix(
    absolutePath: String,
    delimiterOption: Option[String] = Some("/")
  ): Iterator[String] = {
    import scala.collection.JavaConverters._
    S3Objects.withPrefix(amazonS3, s3Bucket, absolutePath).iterator().asScala.map(_.getKey())
  }

  // private API

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

  def richClient(implicit ec: ExecutionContext): AmazonS3ScalaClient = richClientCache.get(ec)

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

  // Note: All async methods that use an EC take it directly as an implicit parameter on the method, not the object.
  // No synchronous operations use the EC, threads or futures.
  // The sync client is made from the async one that is linked to the global EC for guaranteed symmetry,
  // and to prevent code duplication.
  lazy val s3Client: AmazonS3 = richClient(scala.concurrent.ExecutionContext.global).client

  import io.circe._

  private val encoder: Encoder[S3Store] =
    Encoder.forProduct1("basePath") {
      obj => Tuple1(obj.basePath)
    }

  private val decoder: Decoder[S3Store] =
    Decoder.forProduct1("basePath") {
      basePath: String => new S3Store(basePath)
    }

  implicit val codec: Codec[S3Store] = Codec(encoder, decoder)
}
