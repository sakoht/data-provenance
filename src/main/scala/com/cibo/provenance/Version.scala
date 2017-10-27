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

case class Version(id: String) {
  override def toString = f"v$id"
}


