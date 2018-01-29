package com.cibo.provenance

import java.time.Instant

/**
  * Created by ssmith on 5/11/17.
  *
  * This is wrapper around version information for a FunctionWithProvenance.
  *
  * The full version of data is a combination of this "logical" version,
  * and the BuildInfo.
  *
  */

case class Version(id: String, dev: Boolean = false) extends Serializable {
  override def toString: String = f"v$id" + (if (dev) DevVersion.suffix else "")
}

object NoVersion extends Version("-") with Serializable

object NoVersionProvenance extends UnknownProvenance[Version](NoVersion) with Serializable

object DevVersion {
  lazy val suffix = "-dev-" + Instant.now.toString + "-" + sys.env.get("USER")
}
