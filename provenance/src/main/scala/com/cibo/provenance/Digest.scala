package com.cibo.provenance

/**
  * Created by ssmith on 10/9/17.
  *
  * This is a wrapper for the hex string of the SHA1 of a VirtualOutput.
  * The digest is used as identity and a storage key.
  *
  */

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

case class Digest(id: String)
