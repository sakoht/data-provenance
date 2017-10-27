package com.cibo.provenance

/**
  * Created by ssmith on 10/9/17.
  *
  * This is a wrapper for the hex string of the SHA1 of a VirtualOutput.
  * The digest is used as identity and a storage key.
  *
  */

case class Digest(value: String)
