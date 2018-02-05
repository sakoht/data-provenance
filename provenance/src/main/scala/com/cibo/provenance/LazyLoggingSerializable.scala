package com.cibo.provenance

trait LazyLoggingSerializable {
  import com.typesafe.scalalogging.Logger
  import org.slf4j.LoggerFactory

  @transient
  protected lazy val logger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getName))
}
