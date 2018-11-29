package com.cibo.provenance

import com.google.common.cache._
import org.slf4j.{Logger, LoggerFactory}

/**
  * CacheUtils centralizes functions that create a Guava cache.
  *
  * This is a narrow use-case of the more general, flexible GCache used internally at CiBO.
  * If that is released at some point, switch to that.
  */
object CacheUtils {

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def mkCacheBuilder[K, V](maxSize: Long, logger: Logger = logger): CacheBuilder[K, V] =
    CacheBuilder.newBuilder().asInstanceOf[CacheBuilder[K,V]]
      .maximumSize(maxSize)
      .removalListener(new RemovalListener[K, V] {
        override def onRemoval(notification: RemovalNotification[K, V]): Unit = {
          logger.debug(s"Removing key ${Option(notification.getKey)} from cache. Reason: ${Option(notification.getCause)}")
        }
      })

  def mkCache[K, V](maxSize: Long, logger: Logger = logger): Cache[K, V] =
    mkCacheBuilder[K, V](maxSize, logger).build[K, V]

  def mkLoadingCache[K, V](maxSize: Long, logger: Logger = logger)(fn: K => V): LoadingCache[K, V] = {
    val loader = new CacheLoader[K, V] { override def load(key: K): V = fn(key) }
    mkCacheBuilder[K, V](maxSize, logger).build[K, V](loader)
  }
}
