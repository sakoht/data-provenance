package com.cibo.provenance.kvstore

import com.amazonaws.services.s3.AmazonS3
import com.cibo.provenance.Codec
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.{ExecutionContext, Future}


/**
  * KVStore is an abstraction over a key-value store used by ResultTrackers.
  *
  * The native implementation is for local files (LocalStore) and S3.
  * Any other key-value store that supports listing keys by slash-separated prefix will work.
  *
  */

trait KVStore {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def basePath: String

  def isLocal: Boolean

  def isRemote: Boolean

  def exists(key: String): Boolean

  def putBytes(key: String, value: Array[Byte], contentTypeOption: Option[String] = None): Unit

  def getBytes(key: String): Array[Byte]

  def putBytesAsync(key: String, value: Array[Byte], contentTypeOption: Option[String] = None)(implicit ec: ExecutionContext): Future[Any]

  def getBytesAsync(key: String)(implicit ec: ExecutionContext): Future[Array[Byte]]

  def getKeySuffixes(keyPrefix: String = "", delimiterOption: Option[String] = None): Iterable[String]
}


object KVStore {

  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * The apply method constructs a KVStore of any of the default types based on the specified path String.
    * - S3Store
    * - LocalStore
    *
    * Other subtypes of KVStore must constructed by name.
    *
    * @param basePath   The stringified path.
    * @param s3         An implicit AmazonS3 to be used with S3 paths.
    * @return           A KVStore.
    */
  def apply(basePath: String)(implicit s3: AmazonS3 = S3Store.amazonS3): KVStore =
    basePath match {
      case p if p.startsWith("s3://") => new S3Store(p)(s3)
      case p if p.startsWith("/") => new LocalStore(p)
      case other =>
        throw new RuntimeException(
          f"Failed to recognize $other as one of the default KVStore types!  " +
          "Use either an \"s3://\" path or a fully qualified *nix path starting with \"/\".  " +
          "Other subtypes of KVStore are not constructed automatically at this version.")
    }

  /**
    * All KVStore objects should be serializable.  This means they require a codec.
    */
  implicit val codec: Codec[KVStore] = Codec.createAbstractCodec[KVStore]()
}





