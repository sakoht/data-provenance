package com.cibo.provenance

/**
  * Created by ssmith on 10/11/17.
  */

class InconsistentVersionException(
  functionName: String,
  version: Version,
  commits: Seq[String],
  inputGroupIdOption: Option[Digest]
) extends RuntimeException(f"Function $functionName version $version at: $commits (inputs: $inputGroupIdOption)")
