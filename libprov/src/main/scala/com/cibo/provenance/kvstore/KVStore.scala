package com.cibo.provenance.kvstore

import com.amazonaws.services.s3.AmazonS3
import com.cibo.provenance.Codec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}


/**
  * KVStore is an abstraction over a key-value store used by ResultTrackers.
  */

object KVStore {

  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * The apply method constructs a KVStore of any of the default types: S3Store or LocalStore,
    * based on the specified path String.
    *
    * @param basePath
    * @param s3
    * @return
    */
  def apply(basePath: String)(implicit s3: AmazonS3 = S3Store.s3Client): KVStore =
    basePath match {
      case p if p.startsWith("s3://") => new S3Store(p)(s3)
      case p if p.startsWith("/") => new LocalStore(p)
    }

  implicit val codec: Codec[KVStore] = Codec.createAbstractCodec[KVStore]()
}

trait KVStore {

  // public API

  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def basePath: String

  def isLocal: Boolean

  def isRemote: Boolean

  def exists(key: String): Boolean

  def putBytes(key: String, value: Array[Byte]): Unit =
    putBytesForAbsolutePath(getAbsolutePathForKey(key), value)

  def getBytes(key: String): Array[Byte] =
    getBytesForAbsolutePath(getAbsolutePathForKey(key))

  def putBytesAsync(key: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] =
    putBytesForAbsolutePathAsync(getAbsolutePathForKey(key), value)

  def getBytesAsync(key: String)(implicit ec: ExecutionContext): Future[Array[Byte]] =
    getBytesForAbsolutePathAsync(getAbsolutePathForKey(key))

  def getKeySuffixes(
    keyPrefix: String = "",
    delimiterOption: Option[String] = None
  ): Iterator[String] = {
    val fullPrefix = getAbsolutePathForKey(keyPrefix)
    val offset: Int =
      if (fullPrefix.endsWith("/"))
        fullPrefix.length
      else
        fullPrefix.length + 1
    getAbsolutePathsForRelativePrefix(keyPrefix, delimiterOption).map { fullPath =>
      // This has more sanity checking that should be necessary, but one-off errors have crept in several times.
      assert(fullPath.startsWith(fullPrefix), s"Full path '$fullPath' does not start with the expected prefix '$fullPrefix' (from $keyPrefix)")
      fullPath
        .substring(offset)
        .ensuring(!_.startsWith("/"), s"Full path suffix should not start with a '/'")
    }
  }

  // protected methods implemented by subclasses

  protected def getAbsolutePathForKey(key: String): String


  protected def putBytesForAbsolutePath(absolutePath: String, value: Array[Byte]): Unit

  protected def getBytesForAbsolutePath(absolutePath: String): Array[Byte]


  protected def putBytesForAbsolutePathAsync(absolutePath: String, value: Array[Byte])(implicit ec: ExecutionContext): Future[Unit]

  protected def getBytesForAbsolutePathAsync(absolutePath: String)(implicit ec: ExecutionContext): Future[Array[Byte]]


  protected[provenance] def remove(key: String): Unit

  protected def getAbsolutePathsForAbsolutePrefix(
    absolutePrefix: String,
    delimiterOption: Option[String] = Some("/")
  ): Iterator[String]

  // private API

  private def getAbsolutePathsForRelativePrefix(relativePrefix: String, delimiterOption: Option[String] = Some("/")): Iterator[String] = {
    val absolutePrefix = getAbsolutePathForKey(relativePrefix)
    getAbsolutePathsForAbsolutePrefix(absolutePrefix, delimiterOption)
  }
}




