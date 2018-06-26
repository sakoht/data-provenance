package com.cibo.provenance

/**
  * Created by ssmith on 2/15/18.
  *
  * Lazy logging with the lazy logger flagged as transient, allowing classes to serialize.
  *
  */
trait LazyLoggingSerializable {
  import com.typesafe.scalalogging.Logger
  import org.slf4j.LoggerFactory

  @transient
  protected lazy val logger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getName))
}
