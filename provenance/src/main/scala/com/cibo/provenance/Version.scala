package com.cibo.provenance

/**
  * Created by ssmith on 5/11/17.
  *
  * This is wrapper around version information for a FunctionWithProvenance.
  *
  * The full version of data is a combination of this "logical" version,
  * and the BuildInfo.
  *
  */

case class Version(id: String) extends Serializable {
  override def toString: String = f"v$id"
}

object NoVersion extends Version("-") with Serializable

object NoVersionProvenance extends UnknownProvenance[Version](NoVersion) with Serializable


